package node

import "context"

type Node interface {
	// IsLeader represent if this node is leader.
	IsLeader() bool
	// WaitForLeaderChange use for obverse cluster`s leader change.
	WaitForLeaderChange() <-chan string
	// Leader return the cluster`s leader address.
	Leader(ctx context.Context) (string, error)
	// Address return the node address.
	Address() string
}
