package server

import (
	"chubbylock/api"
	"chubbylock/config"
	"chubbylock/store"
	"log"
	"net"
	"net/rpc"
	"os"
    "time"
)

// ServerApp defines the Chubby server application.
type ServerApp struct {
	listener net.Listener

	store *store.Store // Raft-backed store.

	logger *log.Logger

	address string // Current Node's RPC Listen Address.

	locks map[api.FilePath]*Lock // In-memory struct of locks.

	sessions map[api.ClientID]*Session // In-memory struct of sessions.

	handler *Handler
    
    leaseDuration time.Duration
}

// app is the global instance of the Chubby server application.
var app *ServerApp

// Run initializes and starts the Chubby server.
func Run(conf *config.Config) {
	var err error

	// Initialize app struct.
	app = &ServerApp{
		logger:   log.New(os.Stderr, "[server] ", log.LstdFlags),
		store:    store.New(conf.RaftDir, conf.RaftBind, conf.InMem),
		address:  conf.Listen,
		locks:    make(map[api.FilePath]*Lock),
		sessions: make(map[api.ClientID]*Session),
        leaseDuration: conf.LeaseDuration,
	}

	// Open the Raft store.
	err = app.store.Open(conf.Single, conf.NodeID)
	if err != nil {
		log.Fatalf("Failed to open store: %v", err)
	}

	// Handle joining an existing cluster.
	if conf.Join != "" {
		client, err := rpc.Dial("tcp", conf.Join)
		if err != nil {
			log.Fatalf("Failed to dial join address %s: %v", conf.Join, err)
		}
		defer client.Close()

		app.logger.Printf("Attempting to join cluster at %s as node %s with Raft address %s",
			conf.Join, conf.NodeID, conf.RaftBind)

		var req JoinRequest
		var resp JoinResponse

		req.RaftAddr = conf.RaftBind
		req.NodeID = conf.NodeID

		err = client.Call("Handler.Join", req, &resp)
		if err != nil {
			log.Fatalf("Failed to call Join RPC on %s: %v", conf.Join, err)
		}
		if resp.Error != nil {
			log.Fatalf("Failed to join cluster: %v", resp.Error)
		}
		app.logger.Printf("Successfully joined cluster.")
	} else if conf.Single {
		app.logger.Println("Bootstrapped as single-node cluster.")
	} else {
		app.logger.Println("Starting and waiting to form or join a cluster.")
	}

	// Register RPC handlers.
	handler := new(Handler)
	err = rpc.Register(handler)
	if err != nil {
		log.Fatalf("Failed to register RPC handlers: %v", err)
	}

	// Listen for client connections.
	app.listener, err = net.Listen("tcp", conf.Listen)
	if err != nil {
		log.Fatalf("Failed to listen on %s: %v", conf.Listen, err)
	}
	app.logger.Printf("Listening for client RPC connections on %s", conf.Listen)

	// Accept incoming RPC connections. This will block until the listener is closed.
	rpc.Accept(app.listener)

	app.logger.Println("RPC listener stopped.")
}

// JoinRequest defines the request body for joining a node (used internally by servers).
type JoinRequest struct {
	RaftAddr string
	NodeID   string
}

// JoinResponse defines the response body for joining a node (used internally by servers).
type JoinResponse struct {
	Error error
}

// RegisterSession registers a new client session.
func (h *Handler) RegisterSession(req api.RegisterSessionRequest, res *api.RegisterSessionResponse) error {
	app.sessions[req.ClientID] = &Session{} // or NewSession() if defined
	app.logger.Printf("Registered session for client %s", req.ClientID)
	return nil
}
