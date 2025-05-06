package config

import (
	"time" 
)

// Config holds the Chubby server configuration.
type Config struct {
	Listen        string
	RaftDir       string
	RaftBind      string
	NodeID        string
	Join          string
	InMem         bool
	Single        bool
	LeaseDuration time.Duration 
}

// NewConfig creates a new Config struct.
func NewConfig(listen, raftDir, raftBind, nodeId, join string, inmem, single bool, leaseDuration time.Duration) *Config { 
	return &Config{
		Listen:        listen,
		RaftDir:       raftDir,
		RaftBind:      raftBind,
		NodeID:        nodeId,
		Join:          join,
		InMem:         inmem,
		Single:        single,
		LeaseDuration: leaseDuration, 
	}
}
