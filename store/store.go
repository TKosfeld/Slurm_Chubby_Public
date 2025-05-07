// Adapted From https://github.com/otoolep/hraftd/blob/master/store/store.go and https://github.com/sherrybai/chubby/blob/master/chubby/store/store.go

// We want our distributed lock service based on Raft consensus. It is easier to implement with existing packages. 
package store

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
)


const (
	retainSnapshotCount = 2
	raftTimeout         = 10 * time.Second
)

type command struct {
	Op    string `json:"op,omitempty"`
	Key   string `json:"key,omitempty"`
	Value string `json:"value,omitempty"`
}

// Defines a Raft-backed key-value storage
type Store struct {
	RaftDir		string       		// Raft storage directory
	RaftBind	string       		// Raft bind address
	Raft		*raft.Raft   		// Raft instance
	inmem 		bool         		// Whether storage is in-memory

	mu			sync.Mutex   		// Lock for synchronizing API operations
	m			map[string]string	// Key-value store for the system

	logger		*log.Logger  		// Logger
}

func New(raftDir string, raftBind string, inmem bool) *Store {
	return &Store{
		RaftDir: 	raftDir,
		RaftBind: 	raftBind,
		m:			make(map[string]string),
		inmem:		inmem,
		logger: 	log.New(os.Stderr, "[store] ",  log.LstdFlags),
	}
}

/*
Opens the store. If enables single node, and there are no existing peers,
then this node becomes the first node, and therefore leader.
nodeID should be the server identifier for this node.
*/
func (store *Store) Open(enableSingleNode bool, nodeID string) error {
	// Initialize Raft configuration
	raftConfig := raft.DefaultConfig()
	raftConfig.LocalID = raft.ServerID(nodeID)

	// Resolve TCP address for Raft communication
	tcpAddr, resolveErr := net.ResolveTCPAddr("tcp", store.RaftBind)
	if resolveErr != nil {
		return resolveErr
	}

	// Setup Raft transport layer
	raftTransport, transportErr := raft.NewTCPTransport(store.RaftBind, tcpAddr, 3, 10*time.Second, os.Stderr)
	if transportErr != nil {
		return transportErr
	}

	// Setup snapshot store to enable Raft log truncation
	snapshotStore, snapErr := raft.NewFileSnapshotStore(store.RaftDir, retainSnapshotCount, os.Stderr)
	if snapErr != nil {
		return fmt.Errorf("error creating snapshot store: %s", snapErr)
	}

	// Setup storage for Raft logs and state
	var log raft.LogStore
	var stable raft.StableStore

	if store.inmem {
		inMemoryStore := raft.NewInmemStore()
		log = inMemoryStore
		stable = inMemoryStore
	} else {
		dbPath := filepath.Join(store.RaftDir, "raft.db")
		boltStore, dbErr := raftboltdb.NewBoltStore(dbPath)
		if dbErr != nil {
			return fmt.Errorf("error creating BoltDB store: %s", dbErr)
		}
		log = boltStore
		stable = boltStore
	}

	// Create Raft instance
	raftNode, raftErr := raft.NewRaft(raftConfig, (*fsm)(store), log, stable, snapshotStore, raftTransport)
	if raftErr != nil {
		return fmt.Errorf("error initializing Raft: %s", raftErr)
	}
	store.Raft = raftNode

	// Bootstrap cluster if this is the first node
	if enableSingleNode {
		initialConfig := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      raftConfig.LocalID,
					Address: raftTransport.LocalAddr(),
				},
			},
		}
		raftNode.BootstrapCluster(initialConfig)
	}

	return nil
}


// Assigns a value to the given key via a Raft log entry.
// This operation is only permitted on the current leader.
func (store *Store) Set(k, v string) error {
	if store.Raft.State() != raft.Leader {
		return fmt.Errorf("operation denied: node is not the leader")
	}

	cmd := &command{
		Op:    "set",
		Key:   k,
		Value: v,
	}
	data, marshalErr := json.Marshal(cmd)
	if marshalErr != nil {
		return marshalErr
	}

	applyFuture := store.Raft.Apply(data, raftTimeout)
	return applyFuture.Error()
}

// Removes the specified key from the store via a Raft log entry.
// This operation is only permitted on the current leader.
func (store *Store) Delete(k string) error {
	if store.Raft.State() != raft.Leader {
		return fmt.Errorf("operation denied: node is not the leader")
	}

	cmd := &command{
		Op:  "delete",
		Key: k,
	}
	data, marshalErr := json.Marshal(cmd)
	if marshalErr != nil {
		return marshalErr
	}

	applyFuture := store.Raft.Apply(data, raftTimeout)
	return applyFuture.Error()
}

// Retrieve the value associated with the specified key.
// Returns an error if the key is not present in the store.
func (store *Store) Get(k string) (string, error) {
	store.mu.Lock()
	defer store.mu.Unlock()

	val, found := store.m[k]
	if !found {
		return "", fmt.Errorf("key %s not found", k)
	}
	return val, nil
}


/* Join integrates a new node, identified by nodeID and reachable at addr, into the Raft cluster.
This method is intended to be called only by the leader node. */
func (s *Store) Join(nodeID, addr string) error {
	s.logger.Printf("processing join request: nodeID=%s, address=%s", nodeID, addr)

	// Retrieve the current Raft cluster configuration
	cfgFuture := s.Raft.GetConfiguration()
	if err := cfgFuture.Error(); err != nil {
		s.logger.Printf("unable to fetch Raft configuration: %v", err)
		return err
	}

	// Check for existing servers with the same ID or address
	for _, member := range cfgFuture.Configuration().Servers {
		idMatches := member.ID == raft.ServerID(nodeID)
		addrMatches := member.Address == raft.ServerAddress(addr)

		switch {
		case idMatches && addrMatches:
			// Node is already in the cluster with matching ID and address
			s.logger.Printf("node %s at %s already part of cluster; skipping join", nodeID, addr)
			return nil
		case idMatches || addrMatches:
			// Conflicting ID or address — remove the old entry before re-adding
			removeFuture := s.Raft.RemoveServer(member.ID, 0, 0)
			if removeErr := removeFuture.Error(); removeErr != nil {
				return fmt.Errorf("failed to remove conflicting node %s at %s: %v", nodeID, addr, removeErr)
			}
		}
	}

	// Add the new node as a voter in the cluster
	addFuture := s.Raft.AddVoter(raft.ServerID(nodeID), raft.ServerAddress(addr), 0, 0)
	if err := addFuture.Error(); err != nil {
		return err
	}

	s.logger.Printf("node %s at %s joined the cluster successfully", nodeID, addr)
	return nil
}


// Implement interface for type FSM.
type fsm Store

func (f *fsm) applySet(key, value string) interface{} {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.m[key] = value
	return nil
}

func (f *fsm) applyDelete(key string) interface{} {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.m, key)
	return nil
}

// Apply applies a Raft log entry to the key-value store.
func (f *fsm) Apply(l *raft.Log) interface{} {
	var c command
	if err := json.Unmarshal(l.Data, &c); err != nil {
		panic(fmt.Sprintf("failed to unmarshal command: %s", err.Error()))
	}

	switch c.Op {
	case "set":
		return f.applySet(c.Key, c.Value)
	case "delete":
		return f.applyDelete(c.Key)
	default:
		panic(fmt.Sprintf("unrecognized command op: %s", c.Op))
	}
}

// Snapshot returns a snapshot of the key-value store.
func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	// Clone the map.
	o := make(map[string]string)
	for k, v := range f.m {
		o[k] = v
	}
	return &fsmSnapshot{store: o}, nil
}

// Restore stores the key-value store to a previous state.
func (f *fsm) Restore(rc io.ReadCloser) error {
	o := make(map[string]string)
	if err := json.NewDecoder(rc).Decode(&o); err != nil {
		return err
	}

	// Set the state from the snapshot, no lock required according to
	// Hashicorp docs.
	f.m = o
	return nil
}



// Implement interface for type FSMSnapshot.
type fsmSnapshot struct {
	store map[string]string
}

/* Persist writes the current snapshot of the state machine to the provided snapshot sink.
It serializes the in-memory store into JSON and writes it to the sink. 
If any step fails, the sink is canceled to prevent Raft from saving a partial snapshot. */
func (f *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	persistErr := func() error {
		// Convert the store's state to a JSON byte slice.
		data, marshalErr := json.Marshal(f.store)
		if marshalErr != nil {
			return marshalErr
		}

		// Write the serialized snapshot data to the sink.
		if _, writeErr := sink.Write(data); writeErr != nil {
			return writeErr
		}

		// Close the sink to indicate snapshot write is complete and valid.
		return sink.Close()
	}()

	// If an error occurred during persistence, cancel the snapshot to prevent corruption.
	if persistErr != nil {
		_ = sink.Cancel()
	}

	return persistErr
}

func (f *fsmSnapshot) Release() {}
