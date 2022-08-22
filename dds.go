package dds

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"runtime"
	"sync"
	"time"

	"dds/manager"
	"dds/mq"
	"dds/node"
	"dds/timingwheel"
	"dds/timingwheel/bucket"
	"dds/timingwheel/job"
	"dds/timingwheel/timingqueue"
	"dds/workerpool"

	"github.com/go-redis/redis/v8"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

// DDS represent Distributed Delay Scheduler.
type DDS struct {
	sync.RWMutex

	exit         chan struct{}
	internalExit chan struct{}

	clusterReady bool

	producerCfg mq.Config

	logger *zap.Logger

	redisClient *redis.ClusterClient

	// node represent current node status.
	node node.Node
	// tw is not nil only when the node is leader, else tw is no use.
	tw *timingwheel.TimingWheel
	//
	producer mq.Producer

	server *grpc.Server
	client *grpc.ClientConn
	manager.UnimplementedManagerServer
}

// TODO: 各个组建的退出处理

// New return a Dispatch instance.
func New(exit chan struct{}, logger *zap.Logger, redisClient *redis.ClusterClient, node node.Node, cfg mq.Config) (*DDS, error) {
	ds := &DDS{
		exit:        exit,
		logger:      logger,
		node:        node,
		producerCfg: cfg,
		redisClient: redisClient,
	}

	go ds.start()
	return ds, nil
}

// watch cluster node state change.
func (ds *DDS) start() {
	ds.logger.Info("[DDS] start, waiting for leader change")
	for {
		select {
		case leader := <-ds.node.WaitForLeaderChange():
		retry:
			ds.release()
			var err error
			if ds.node.IsLeader() {
				go ds.startServer(leader)
				ds.producer, err = mq.NewProducer(ds.producerCfg, ds.logger)
				if err != nil {
					ds.logger.Error("[DDS] start new `Producer`", zap.Error(err))
					goto retry
				}
				workers := workerpool.NewShardPool(256, runtime.NumCPU(), ds.logger)
				ds.internalExit = make(chan struct{})
				ds.tw, err = timingwheel.New(ds.internalExit, workers, ds.redisClient, ds.logger, ds.process)
				if err != nil {
					ds.logger.Error("[DDS] start new `TimingWheel`", zap.Error(err))
					goto retry
				}
			} else {
				ds.client, err = grpc.Dial(leader)
				if err != nil {
					ds.logger.Error("[DDS] start `Dial`", zap.Error(err))
					goto retry
				}
			}

			ds.setState(true)
		case <-ds.exit:
			ds.logger.Info("[DDS] start exit")
			return
		}
	}
}

func (ds *DDS) process(element timingqueue.Element) {
	bucket.New(ds.redisClient, element.Value).Flush(func(id string) {
		jobByte, err := job.GetBytes(id)
		if err != nil {
			ds.logger.Error("[DDS] process bucket flush `GetBytes`", zap.Error(err))
			return
		}

		obj := &job.Job{}
		if err := json.Unmarshal(jobByte, obj); err != nil {
			ds.logger.Error("[DDS] process bucket flush `Unmarshal`", zap.Error(err), zap.String("data", string(jobByte)))
			return
		}

		if obj.State == job.Deleted {
			return
		}

		now := time.Now().Unix()
		expect := obj.NextExecuteTime.Unix()
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if now+1 >= expect {
			// invoke
			if err := obj.SetState(ctx, job.Started); err != nil {
				ds.logger.Error("[DDS] process bucket flush `SetState`", zap.Error(err))
			}

			if err := ds.producer.Product(context.Background(), jobByte); err != nil {
				ds.logger.Error("[DDS] process bucket flush `Product`", zap.Error(err))
				return
			}

			// parse next execute time
			if schedule, err := job.Parse(obj.ExecuteTimeSpec); err == nil {
				obj.LastExecuteTime = obj.NextExecuteTime
				obj.NextExecuteTime = schedule.Next(time.Now())
				if obj.NextExecuteTime.After(time.Now()) {
					if err := ds.tw.Add(ctx, *obj); err != nil {
						ds.logger.Error("[DDS] process bucket flush `Add`", zap.Error(err))
						return
					}
				}
			} else {
				ds.logger.Error("[DDS] process bucket flush `Parse`", zap.Error(err))
				return
			}
		} else {
			// unexpired re add
			if err := ds.tw.Add(ctx, *obj); err != nil {
				ds.logger.Error("[DDS] process bucket flush re `Add`", zap.Error(err))
				return
			}
		}
	})
}

// serveGRPC used for leader start grpc server.
func (ds *DDS) startServer(addr string) {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		ds.logger.Error("[DDS] start internal GRPC `Listen`", zap.Error(err))
	}
	defer lis.Close()

	ds.server = grpc.NewServer()
	manager.RegisterManagerServer(ds.server, ds)
	if err := ds.server.Serve(lis); err != nil {
		ds.logger.Error("[DDS] start internal GRPC `Serve`", zap.Error(err))
	}
}

func (ds *DDS) isReady() bool {
	ds.RLock()
	defer ds.RUnlock()
	return ds.clusterReady
}

func (ds *DDS) setState(ready bool) {
	ds.Lock()
	defer ds.Unlock()
	ds.clusterReady = ready
}

func (ds *DDS) release() {
	if ds.server != nil {
		ds.server.GracefulStop()
		ds.server = nil
	}

	if ds.client != nil {
		ds.client.Close()
		ds.client = nil
	}

	if ds.tw != nil {
		close(ds.internalExit)
		ds.tw = nil
		ds.internalExit = nil
	}

	if ds.producer != nil {
		ds.producer = nil
	}

	ds.setState(false)
}

// TODO: 如果在 schedule 的同时发生 leader 变更，需要等待 leader 初始化完成

// Schedule a job to cluster.
func (ds *DDS) Schedule(ctx context.Context, spec string, data []byte) (string, error) {
	if !ds.isReady() {
		return "", errors.New("cluster not ready, please try again later")
	}

	if !ds.node.IsLeader() {
		reply, err := manager.NewManagerClient(ds.client).Add(ctx, &manager.AddRequest{
			Data: data,
			Spec: spec,
		})
		if err != nil {
			return "", err
		}
		return reply.Id, nil
	}

	instance, err := job.Create(spec, data)
	if err != nil {
		return "", err
	}
	return instance.Id, ds.tw.Add(ctx, instance)
}

func (ds *DDS) DeleteById(ctx context.Context, id string) error {
	ins, err := job.Get(id)
	if err != nil {
		return err
	}

	return ins.SetState(ctx, job.Deleted)
}

func (ds *DDS) Add(ctx context.Context, request *manager.AddRequest) (*manager.AddReply, error) {
	instance, err := job.Create(request.Spec, request.Data)
	if err != nil {
		return nil, err
	}

	// double check
	if ds.node.IsLeader() {
		if err := ds.tw.Add(ctx, instance); err != nil {
			return nil, err
		}
		return &manager.AddReply{Id: instance.Id}, nil
	} else {
		return manager.NewManagerClient(ds.client).Add(ctx, request)
	}
}

func (ds *DDS) Delete(ctx context.Context, request *manager.DeleteRequest) (*emptypb.Empty, error) {
	instance, err := job.Get(request.Id)
	if err != nil {
		return nil, err
	}

	return nil, instance.SetState(ctx, job.Deleted)
}
