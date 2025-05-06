package main

import (
	"chubbylock/config"
	"chubbylock/server"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"
)

var (
	listen        string        // Server listen address (host:port) for client RPC.
	raftDir       string        // Directory for Raft data (logs, snapshots).
	raftBind      string        // Address (host:port) for Raft communication.
	nodeId        string        // Unique ID for this Chubby server node.
	join          string        // Address (host:port) of an existing Chubby server to join (RPC listen address).
	inmem         bool          // Use in-memory storage for Raft (for testing, not recommended for production).
	single        bool          // Bootstrap as a single-node cluster (for the initial leader).
	leaseDuration int64       //  Changed to int64
)

func init() {
	flag.StringVar(&listen, "listen", ":8000", "Server listen address (host:port) for client RPC")
	flag.StringVar(&raftDir, "raftdir", "./raft-data", "Directory for Raft data")
	flag.StringVar(&raftBind, "raftbind", ":9000", "Address (host:port) for Raft communication")
	flag.StringVar(&nodeId, "id", "", "Unique ID for this Chubby server node (auto-generated if empty in Slurm)")
	flag.StringVar(&join, "join", "", "Address (host:port) of an existing Chubby server to join (RPC listen address)")
	flag.BoolVar(&inmem, "inmem", false, "Use in-memory storage for Raft (testing only)")
	flag.BoolVar(&single, "single", false, "Bootstrap as a single-node cluster (for the initial leader)")
	flag.Int64Var(&leaseDuration, "lease_duration", 15, "Initial lease duration for client sessions in seconds") // Changed to Int64Var
}

func main() {
	flag.Parse()

	var cfg *config.Config

	// Determine Node ID, prioritizing flag, then Slurm env, then default hostname.
	if nodeId == "" {
		jobID := os.Getenv("SLURM_JOBID")
		procID := os.Getenv("SLURM_PROCID")
		hostname, _ := os.Hostname()
		if jobID != "" && procID != "" {
			nodeId = fmt.Sprintf("slurm-%s-%s", jobID, procID)
		} else {
			nodeId = hostname
		}
	}

	cfg = config.NewConfig(listen, raftDir, raftBind, nodeId, join, inmem, single, time.Duration(leaseDuration)*time.Second) // Convert to time.Duration

	quitCh := make(chan os.Signal, 1)
	signal.Notify(quitCh, os.Kill, os.Interrupt, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

	// Run the server.
	go server.Run(cfg)

	fmt.Printf("Chubby server started with ID: %s, Listen: %s, Raft Bind: %s, Raft Dir: %s, Join: %s, In-memory: %v, Single-node: %v, Lease Duration: %s\n",
		cfg.NodeID, cfg.Listen, cfg.RaftBind, cfg.RaftDir, cfg.Join, cfg.InMem, single, cfg.LeaseDuration) 
	// Exit on signal.
	<-quitCh
	fmt.Println("Chubby server shutting down...")
}