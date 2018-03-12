package cluster

import "github.com/lodastack/store/log"

// Options are the options to be used when initializing a cluster service.
type Options struct {
	// Bind address to use for the cluster raft service.
	Bind string

	// DataDir is the directory where the data stores.
	DataDir string

	// JoinAddr, which cluster to join.
	// Optional.
	JoinAddr string

	// Logger is cluster store logger
	Logger log.Logger
}
