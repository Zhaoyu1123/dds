package node

import (
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

func TestExample(t *testing.T) {
	var node1, node2, node3 Node
	var err error
	t.Log(os.Getenv("ETCD_ENDPOINTS"), 111)
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   strings.Split(os.Getenv("ETCD_ENDPOINTS"), ","),
		DialTimeout: 5 * time.Second,
	})
	require.Nil(t, err)

	exit1 := make(chan struct{})
	exit2 := make(chan struct{})
	exit3 := make(chan struct{})

	logger := zap.NewExample()
	node1, err = NewNode(exit1, logger, client, "node1:70")
	node2, err = NewNode(exit2, logger, client, "node2:80")
	node3, err = NewNode(exit3, logger, client, "node3:90")
	require.Nil(t, err)

	go func() {
		for leader := range node1.WaitForLeaderChange() {
			t.Log("[node1] current leader is ", leader)
			require.Equal(t, node1.IsLeader(), leader == "node1:70")
		}
		t.Log("node1 exit.")
	}()

	go func() {
		for leader := range node2.WaitForLeaderChange() {
			t.Log("[node2] current leader is ", leader)
			require.Equal(t, node2.IsLeader(), leader == "node2:80")
		}
		t.Log("node2 exit.")
	}()

	go func() {
		for leader := range node3.WaitForLeaderChange() {
			t.Log("[node3] current leader is ", leader)
			require.Equal(t, node3.IsLeader(), leader == "node3:90")
		}
		t.Log("node3 exit.")
	}()

	time.Sleep(5 * time.Second)
	switch {
	case node1.IsLeader():
		close(exit1)
	case node2.IsLeader():
		close(exit2)
	case node3.IsLeader():
		close(exit3)
	}

	time.Sleep(10 * time.Second)
	switch {
	case node1.IsLeader():
		close(exit1)
	case node2.IsLeader():
		close(exit2)
	case node3.IsLeader():
		close(exit3)
	}

	time.Sleep(10 * time.Second)
}
