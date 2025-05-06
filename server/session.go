// server/session.go
package server

import (
	"chubbylock/api"
	"errors"
	"fmt"
	//"log"
	"sync"
	"time"
)

const DefaultLeaseExt = 15 * time.Second

// Session contains metadata for one Chubby session.
type Session struct {
	clientID      api.ClientID
	startTime     time.Time
	leaseLength   time.Duration
	ttlLock       sync.Mutex
	ttlChannel    chan struct{}
	locks         map[api.FilePath]*Lock
	terminated    bool
	terminatedChan chan struct{}
}

// Lock describes information about a particular Chubby lock.
type Lock struct {
	path    api.FilePath
	mode    api.LockMode
	owners  map[api.ClientID]bool
	content string
}

/* Create Session struct. */
func CreateSession(clientID api.ClientID) (*Session, error) {
	sess, ok := app.sessions[clientID]
	if ok {
		return nil, errors.New(fmt.Sprintf("Client %s already has a session", clientID))
	}

	app.logger.Printf("Creating session with client %s", clientID)

	sess = &Session{
		clientID:       clientID,
		startTime:      time.Now(),
		leaseLength:    app.leaseDuration, 
		ttlChannel:     make(chan struct{}, 2),
		locks:          make(map[api.FilePath]*Lock),
		terminated:     false,
		terminatedChan: make(chan struct{}, 2),
	}

	app.sessions[clientID] = sess
	go sess.MonitorSession()

	return sess, nil
}
func (sess *Session) MonitorSession() {
	app.logger.Printf("Monitoring session for client %s", sess.clientID)
	ticker := time.Tick(time.Second)
	for range ticker {
		timeLeaseOver := sess.startTime.Add(sess.leaseLength)
		durationLeaseOver := time.Until(timeLeaseOver)

		if durationLeaseOver <= 0 {
			app.logger.Printf("Lease expired for client %s: terminating", sess.clientID)
			sess.TerminateSession()
			return
		}

		if durationLeaseOver <= (1 * time.Second) {
			select {
			case sess.ttlChannel <- struct{}{}:
			default:
				// Don't block if channel is full
			}
		}
	}
}

// Terminate the session.
func (sess *Session) TerminateSession() {
	if sess.terminated {
		return
	}
	sess.terminated = true
	close(sess.terminatedChan)

	for filePath := range sess.locks {
		err := sess.ReleaseLock(filePath)
		if err != nil {
			app.logger.Printf("Error releasing lock %s for client %s on termination: %v", filePath, sess.clientID, err)
		}
	}
	app.logger.Printf("Terminated session for client %s", sess.clientID)
	delete(app.sessions, sess.clientID) // Clean up session from global map
}

// Extend Lease after receiving keepalive messages
func (sess *Session) KeepAlive(clientID api.ClientID) time.Duration {
	select {
	case <-sess.terminatedChan:
		return 0 // Session already terminated

	case <-sess.ttlChannel:
		sess.leaseLength += app.leaseDuration // Use the global leaseDuration from app
		app.logger.Printf("Session extended for client %s, new lease: %s", sess.clientID, sess.leaseLength)
		return sess.leaseLength
	}
}

// Create the lock if it does not exist in the global lock map.
func (sess *Session) OpenLock(path api.FilePath) error {
	if _, exists := app.locks[path]; !exists {
		app.locks[path] = &Lock{
			path:    path,
			mode:    api.FREE,
			owners:  make(map[api.ClientID]bool),
			content: "",
		}
	}
	sess.locks[path] = app.locks[path] // Ensure session knows about it
	return nil
}

// Delete the lock. Lock must be held in exclusive mode by this session.
func (sess *Session) DeleteLock(path api.FilePath) error {
	_, exists := app.locks[path]
	if !exists || !sess.holdsExclusive(path) {
		return errors.New(fmt.Sprintf("Cannot delete lock %s: not held exclusively by session %s", path, sess.clientID))
	}
	delete(app.locks, path)
	delete(sess.locks, path)
	err := app.store.Delete(string(path))
	return err
}

// Try to acquire the lock.
func (sess *Session) TryAcquireLock(path api.FilePath, mode api.LockMode) (bool, error) {
	lock, exists := app.locks[path]
	if !exists {
		return false, errors.New(fmt.Sprintf("Lock %s has not been opened", path))
	}

	switch lock.mode {
	case api.EXCLUSIVE:
		if _, owned := lock.owners[sess.clientID]; owned {
			return true, nil // Already owns it
		}
		return false, nil // Already held exclusively
	case api.SHARED:
		if mode == api.EXCLUSIVE {
			return false, nil // Cannot acquire exclusive on shared
		}
		lock.owners[sess.clientID] = true
		sess.locks[path] = lock
		return true, nil
	case api.FREE:
		lock.mode = mode
		lock.owners[sess.clientID] = true
		sess.locks[path] = lock
		return true, nil
	default:
		return false, fmt.Errorf("Unknown lock mode: %v", lock.mode)
	}
}

// Release the lock.
func (sess *Session) ReleaseLock(path api.FilePath) error {
	lock, exists := app.locks[path]
	if !exists || !lock.owners[sess.clientID] {
		return errors.New(fmt.Sprintf("Session %s does not hold lock %s", sess.clientID, path))
	}
	delete(lock.owners, sess.clientID)
	if len(lock.owners) == 0 {
		lock.mode = api.FREE
	}
	sess.locks[path] = lock // Update session's view
	app.locks[path] = lock  // Update global view
	return nil
}

// Read the Content of a lockfile.
func (sess *Session) ReadContent(path api.FilePath) (string, error) {
	lock, exists := app.locks[path]
	if !exists || !lock.owners[sess.clientID] {
		return "", errors.New(fmt.Sprintf("Session %s does not hold lock %s to read", sess.clientID, path))
	}
	content, err := app.store.Get(string(path))
	return content, err
}

// Write the Content to a lockfile. Requires exclusive lock.
func (sess *Session) WriteContent(path api.FilePath, content string) error {
	if !sess.holdsExclusive(path) {
		return errors.New(fmt.Sprintf("Session %s does not hold exclusive lock on %s to write", sess.clientID, path))
	}
	err := app.store.Set(string(path), content)
	if err != nil {
		return fmt.Errorf("failed to write content to %s: %v", path, err)
	}
	lock, exists := app.locks[path]
	if exists {
		lock.content = content // Update in-memory content
		app.locks[path] = lock
	}
	return nil
}

// Helper function to check if the session holds an exclusive lock.
func (sess *Session) holdsExclusive(path api.FilePath) bool {
	lock, exists := sess.locks[path]
	return exists && lock.mode == api.EXCLUSIVE && lock.owners[sess.clientID]
}

// RPC handler type
type Handler int

/*
 * Called by servers:
 */

// Join the caller server to our server.
func (h *Handler) Join(req JoinRequest, res *JoinResponse) error {
	err := app.store.Join(req.NodeID, req.RaftAddr)
	res.Error = err
	return err
}

/*
 * Called by clients:
 */

// Initialize a client-server session.
func (h *Handler) InitSession(req api.InitSessionRequest, res *api.InitSessionResponse) error {
	// If a non-leader node receives an InitSession, return error
	if app.store.Raft.State().String() != "Leader" {
		return errors.New(fmt.Sprintf("Node %s is not the leader", app.address))
	}

	_, err := CreateSession(api.ClientID(req.ClientID))
	return err
}

// KeepAlive calls allow the client to extend the Chubby session.
func (h *Handler) KeepAlive(req api.KeepAliveRequest, res *api.KeepAliveResponse) error {
	// If a non-leader node receives a KeepAlive, return error
	if app.store.Raft.State().String() != "Leader" {
		return errors.New(fmt.Sprintf("Node %s is not the leader", app.address))
	}

	var err error
	sess, ok := app.sessions[req.ClientID]
	if !ok {
		// Probably a jeopardy KeepAlive: create a new session for the client
		app.logger.Printf("Client %s sent jeopardy KeepAlive: creating new session", req.ClientID)

		// Note: this starts the local lease countdown
		// Should be ok to not call KeepAlive until later because lease TTL is pretty long (12s)
		sess, err = CreateSession(req.ClientID)
		if err != nil {
			// This shouldn't happen because session shouldn't be in app.sessions struct yet
			return err
		}

		app.logger.Printf("New session for client %s created", req.ClientID)

		// For each lock in the KeepAlive, try to acquire the lock
		// If any of the acquires fail, terminate the session.
		for filePath, lockMode := range req.Locks {
			ok, err := sess.TryAcquireLock(filePath, lockMode)
			if err != nil {
				// Don't return an error because the session won't terminate!
				app.logger.Printf("Error when client %s acquiring lock at %s: %s", req.ClientID, filePath, err.Error())
			}
			if !ok {
				app.logger.Printf("Jeopardy client %s failed to acquire lock at %s", req.ClientID, filePath)
				// This should cause the KeepAlive response to return that session should end.
				sess.TerminateSession()

				return nil // Don't return an error because the session won't terminate!
			}
			app.logger.Printf("Lock %s reacquired successfully.", filePath)
		}

		app.logger.Printf("Finished jeopardy KeepAlive process for client %s", req.ClientID)
	}

	duration := sess.KeepAlive(req.ClientID)
	res.LeaseLength = duration
	return nil
}

// Open a lock.
func (h *Handler) OpenLock(req api.OpenLockRequest, res *api.OpenLockResponse) error {
	sess, ok := app.sessions[req.ClientID]
	if !ok {
		return errors.New(fmt.Sprintf("No session exists for %s", req.ClientID))
	}
	err := sess.OpenLock(req.Filepath)
	if err != nil {
		return err
	}
	return nil
}

// Delete a lock.
func (h *Handler) DeleteLock(req api.DeleteLockRequest, res *api.DeleteLockResponse) error {
	sess, ok := app.sessions[req.ClientID]
	if !ok {
		return errors.New(fmt.Sprintf("No session exists for %s", req.ClientID))
	}
	err := sess.DeleteLock(req.Filepath)
	if err != nil {
		return err
	}
	return nil
}

// Try to acquire a lock.
func (h *Handler) TryAcquireLock(req api.TryAcquireLockRequest, res *api.TryAcquireLockResponse) error {
	sess, ok := app.sessions[req.ClientID]
	if !ok {
		return errors.New(fmt.Sprintf("No session exists for %s", req.ClientID))
	}
	isSuccessful, err := sess.TryAcquireLock(req.Filepath, req.Mode)
	if err != nil {
		return err
	}
	res.IsSuccessful = isSuccessful
	return nil
}

// Release lock.
func (h *Handler) ReleaseLock(req api.ReleaseLockRequest, res *api.ReleaseLockResponse) error {
	sess, ok := app.sessions[req.ClientID]
	if !ok {
		return errors.New(fmt.Sprintf("No session exists for %s", req.ClientID))
	}
	err := sess.ReleaseLock(req.Filepath)
	if err != nil {
		return err
	}
	return nil
}

// Read Content
func (h *Handler) ReadContent(req api.ReadRequest, res *api.ReadResponse) error {
	sess, ok := app.sessions[req.ClientID]
	if !ok {
		return errors.New(fmt.Sprintf("No session exists for %s", req.ClientID))
	}
	content, err := sess.ReadContent(req.Filepath)
	if err != nil {
		return err
	}
	res.Content = content
	return nil
}

// Write Content
func (h *Handler) WriteContent(req api.WriteRequest, res *api.WriteResponse) error {
	sess, ok := app.sessions[req.ClientID]
	if !ok {
		return errors.New(fmt.Sprintf("No session exists for %s", req.ClientID))
	}
	err := sess.WriteContent(req.Filepath, req.Content)
	if err != nil {
		res.IsSuccessful = false
		return err
	}
	res.IsSuccessful = true
	return nil
}