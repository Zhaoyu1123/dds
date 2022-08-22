package node

import (
	"context"
	"fmt"
	"sync"
	"time"

	"dds/utils"

	"go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"go.uber.org/zap"
)

var defaultInterval = 30
var defaultElectionKey = "node"

// Node represent a node in cluster.
type node struct {
	exit chan struct{}

	ctx    context.Context
	cancel context.CancelFunc

	client *clientv3.Client

	logger *zap.Logger

	address string

	isLeader bool
	// only watch the change of node status from leader to follower.
	leaderToFollowerChan chan string
	// watch the change of cluster`s leader.
	leaderChangeC chan string

	// leaseID use for create session.
	leaseID clientv3.LeaseID
	// session keepalive node in etcd.
	session *concurrency.Session
	// election can make node state change from Follower to Leader.
	election *concurrency.Election

	sync.RWMutex
}

// NewNode create a node instance.
// address eg: `10.0.0.xx:8080`
func NewNode(exit chan struct{}, logger *zap.Logger, client *clientv3.Client, address string) (Node, error) {
	ctx, cancel := context.WithCancel(context.Background())
	n := &node{
		exit:                 exit,
		ctx:                  ctx,
		cancel:               cancel,
		client:               client,
		logger:               logger,
		address:              address,
		leaderChangeC:        make(chan string, 0),
		leaderToFollowerChan: make(chan string, 0),
	}

	resp, err := client.Grant(ctx, int64(defaultInterval))
	if err != nil {
		return nil, err
	}

	n.leaseID = resp.ID
	if err := n.newSession(resp.ID, defaultInterval); err != nil {
		return n, err
	}

	go n.obverse()
	go n.join()
	return n, nil
}

// newSession init node`s session with LeaseID, use for node keepalive.
func (n *node) newSession(leaseID clientv3.LeaseID, ttl int) error {
	session, err := concurrency.NewSession(n.client, concurrency.WithTTL(ttl),
		concurrency.WithContext(n.ctx), concurrency.WithLease(leaseID))
	if err != nil {
		return err
	}
	election := concurrency.NewElection(session, defaultElectionKey)
	n.session = session
	n.election = election
	return nil
}

// set node state in cluster.
func (n *node) setState(isLeader bool) {
	// Only report changes in leadership
	if n.isLeader == isLeader {
		return
	}

	n.Lock()
	n.isLeader = isLeader
	n.Unlock()
}

// node join in cluster become a follower or leader.
func (n *node) join() {
	for {
		// Discover who is the leader of this election
		if resp, err := n.election.Leader(n.ctx); err != nil {
			if err == context.Canceled {
				n.logger.Info(fmt.Sprintf("Node[%s] join exit when get `Leader`", n.address))
				return
			}
			if err != concurrency.ErrElectionNoLeader {
				n.logger.Error(fmt.Sprintf("Node[%s] join get `Leader`", n.address), zap.Error(err))
				continue
			}
		} else if string(resp.Kvs[0].Value) == n.address {
			// If we are joining an election from which we previously had leadership we
			// have to inherit the leadership if the lease has not expired. This is a race as the
			// lease could expire in between the `Leader()` call and when we resume
			// observing changes to the election. If this happens we should detect the
			// session has expired during the observation loop.

			// Recreate our session with the old lease id.
			if err = n.newSession(clientv3.LeaseID(resp.Kvs[0].Lease), defaultInterval); err != nil {
				n.logger.Error(fmt.Sprintf("Node[%s] join Re-establishing new session", n.address), zap.Error(err))
				continue
			}
			n.election = concurrency.ResumeElection(n.session, n.address,
				string(resp.Kvs[0].Key), resp.Kvs[0].CreateRevision)

			select {
			case <-n.leaderToFollowerChan: // waiting for leader change to follower, do not need to close this chan.
				n.logger.Info(fmt.Sprintf("Node[%s] join leader change to follower", n.address))
			case <-n.ctx.Done():
				n.logger.Info(fmt.Sprintf("Node[%s] join exit", n.address))
				return
			}
		}

		n.logger.Info(fmt.Sprintf("Node[%s] join start `Campaign`", n.address))
		// Attempt to become leader, block here...
		if err := n.election.Campaign(n.ctx, n.address); err != nil {
			if err == context.Canceled {
				n.logger.Info(fmt.Sprintf("Node[%s] join exit when `Campaign`", n.address))
				return
			}
			// NOTE: Campaign currently does not return an error if session expires
			n.logger.Error(fmt.Sprintf("Node[%s] join `Campaign` for leader", n.address), zap.Error(err))
			n.session.Close()
			continue
		}
	}
}

// observe the state changes of the cluster leader.
func (n *node) obverse() {
	observeResp := n.election.Observe(n.ctx)
	for {
		select {
		case resp, ok := <-observeResp:
			if !ok {
				n.session.Close()
				n.reconnect()
				observeResp = n.election.Observe(n.ctx)
				continue
			}

			leader := string(resp.Kvs[0].Value)
			if leader == n.address {
				n.setState(true)
			} else if n.isLeader {
				n.setState(false)
				n.leaderToFollowerChan <- leader
			}
			n.leaderChangeC <- leader
		case <-n.exit:
			if n.isLeader {
				// If resign takes longer than our TTL then lease is expired, and we are no
				// longer leader anyway.
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				if err := n.election.Resign(ctx); err != nil {
					n.logger.Error(fmt.Sprintf("Node[%s] obverse `Resign`", n.address), zap.Error(err))
				}
				cancel()
			}
			n.setState(false)
			n.session.Close()
			close(n.leaderChangeC)
			n.cancel()
			n.logger.Info(fmt.Sprintf("Node[%s] obverse exit", n.address))
			return
		case <-n.session.Done():
			// NOTE: Observe will not close if the session expires, we must
			// watch for session.Done()
			n.reconnect()
			observeResp = n.election.Observe(n.ctx)
		}
	}
}

// reconnect if node session is unavailable refresh.
func (n *node) reconnect() {
	backOff := 2

	for {
		if err := n.newSession(clientv3.LeaseID(0), defaultInterval); err != nil {
			if err == context.Canceled {
				return
			}

			n.logger.Error(fmt.Sprintf("Node[%s] reconnect new session", n.address), zap.Error(err))

			tick := time.NewTicker(time.Duration(backOff) * time.Second)
			select {
			case <-n.ctx.Done():
				tick.Stop()
				return
			case <-tick.C:
				tick.Stop()
				backOff = utils.FibonacciNext(backOff)
			}
			continue
		}
		break
	}
}

func (n *node) IsLeader() bool {
	n.RLock()
	defer n.RUnlock()
	return n.isLeader
}

func (n *node) WaitForLeaderChange() <-chan string {
	return n.leaderChangeC
}

func (n *node) Leader(ctx context.Context) (string, error) {
	resp, err := n.election.Leader(ctx)
	if err != nil {
		return "", err
	}
	return string(resp.Kvs[0].Value), nil
}

func (n *node) Address() string {
	return n.address
}
