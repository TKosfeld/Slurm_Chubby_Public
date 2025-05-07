// server/session.go
package server

import (
	"chubbylock/api"
	"errors"
	"fmt"
	"time"
)

// Session contains metadata for one Chubby session.
type Session struct {
	clientID   api.ClientID         // Client ID
	startTime  time.Time            // Time of session start
	leaseDur   time.Duration        // Current lease duration for the session
	ttlChannel chan struct{}        // Time to Live Channel
	locks      map[api.FilePath]*Lock // Locks currently held by this session
	terminated bool                 // Indicates whether the session has been terminated
	endChannel chan struct{}        // Channel used to signal ending
}


// Lock describes information about a particular Chubby lock.
type Lock struct {
	path    api.FilePath
	mode    api.LockMode
	owners  map[api.ClientID]bool
	content string
}


// CreateSession initializes a new session for the given client ID.
// Returns an error if a session already exists for the client.
func CreateSession(clientID api.ClientID) (*Session, error) {
	// Check if a session already exists for the client
	if _, exists := app.sessions[clientID]; exists {
		return nil, fmt.Errorf("Client %s already has a session", clientID)
	}

	app.logger.Printf("Starting new session for client %s", clientID)

	// Initialize a new session struct
	newSession := &Session{
		clientID:    clientID,
		startTime:   time.Now(),
		leaseDur:    app.leaseDuration,
		ttlChannel:  make(chan struct{}, 2),               // Buffered to avoid blocking
		locks:       make(map[api.FilePath]*Lock),        // Initialize lock map
		terminated:  false,
		endChannel:  make(chan struct{}, 2),               // Channel to signal session termination
	}

	// Register session in the global session map
	app.sessions[clientID] = newSession

	// Start monitoring this session for lease expiration
	go newSession.MonitorSession()

	return newSession, nil
}

// MonitorSession continuously checks if the session’s lease has expired.
// Sends a signal via ttlChannel when close to expiry, and terminates the session when expired.
func (sess *Session) MonitorSession() {
	app.logger.Printf("Monitoring session for client %s", sess.clientID)

	timeTicker := time.Tick(time.Second)

	for range timeTicker {
		expirationTime := sess.startTime.Add(sess.leaseDur)
		timeLeft := time.Until(expirationTime)

		// If the lease has expired, terminate the session
		if timeLeft <= 0 {
			app.logger.Printf("Lease expired for client %s: terminating session", sess.clientID)
			sess.TerminateSession()
			return
		}

		// If lease is about to expire within 1 second, notify via ttlChannel
		if timeLeft <= time.Second {
			select {
			case sess.ttlChannel <- struct{}{}:
			default:
				// Non-blocking: skip if the channel is full
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
	close(sess.endChannel)

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
	case <-sess.endChannel:
		return 0 // Session already terminated

	case <-sess.ttlChannel:
		sess.leaseDur += app.leaseDuration // Use the global leaseDuration from app
		app.logger.Printf("Session extended for client %s, new lease: %s", sess.clientID, sess.leaseDur)
		return sess.leaseDur
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

// KeepAlive extends the lease for a client’s session.
// If the client is unknown, it may represent a jeopardy client attempting to reestablish a session.
func (h *Handler) KeepAlive(req api.KeepAliveRequest, res *api.KeepAliveResponse) error {
	// Reject KeepAlive if the current node is not the Raft leader
	if app.store.Raft.State().String() != "Leader" {
		return fmt.Errorf("Node %s is not the leader", app.address)
	}

	var err error
	sess, exists := app.sessions[req.ClientID]

	// If session doesn't exist, treat this as a jeopardy KeepAlive
	if !exists {
		app.logger.Printf("Received jeopardy KeepAlive from client %s: creating new session", req.ClientID)

		// Start a new session locally
		sess, err = CreateSession(req.ClientID)
		if err != nil {
			// Should not happen unless there's an unexpected race condition
			return err
		}
		app.logger.Printf("Created new session for jeopardy client %s", req.ClientID)

		// Reacquire all previously held locks for the client
		for filePath, lockMode := range req.Locks {
			acquired, err := sess.TryAcquireLock(filePath, lockMode)
			if err != nil {
				app.logger.Printf("Error reacquiring lock for client %s at %s: %s", req.ClientID, filePath, err.Error())
				// Continue; don't exit early so all reacquire attempts are made
			}
			if !acquired {
				app.logger.Printf("Jeopardy client %s failed to reacquire lock at %s", req.ClientID, filePath)
				sess.TerminateSession()
				return nil // Not an error: client should interpret this as "session ended"
			}
			app.logger.Printf("Successfully reacquired lock on %s", filePath)
		}

		app.logger.Printf("Completed jeopardy KeepAlive for client %s", req.ClientID)
	}

	// Extend the lease and return the new duration
	duration := sess.KeepAlive(req.ClientID)
	res.LeaseDur = duration
	return nil
}


// OpenLock initializes a lock for the specified file path in the current session.
func (h *Handler) OpenLock(req api.OpenLockRequest, res *api.OpenLockResponse) error {
	sess, ok := app.sessions[req.ClientID]
	if !ok {
		return fmt.Errorf("Session not found for client %s", req.ClientID)
	}
	return sess.OpenLock(req.Filepath)
}

// TryAcquireLock attempts to acquire a lock on a given file path in the requested mode.
// The result of the acquisition attempt is returned in the response.
func (h *Handler) TryAcquireLock(req api.TryAcquireLockRequest, res *api.TryAcquireLockResponse) error {
	sess, ok := app.sessions[req.ClientID]
	if !ok {
		return fmt.Errorf("Session not found for client %s", req.ClientID)
	}
	success, err := sess.TryAcquireLock(req.Filepath, req.Mode)
	if err != nil {
		return err
	}
	res.IsSuccessful = success
	return nil
}

// ReleaseLock frees a previously acquired lock on the specified file path.
func (h *Handler) ReleaseLock(req api.ReleaseLockRequest, res *api.ReleaseLockResponse) error {
	sess, ok := app.sessions[req.ClientID]
	if !ok {
		return fmt.Errorf("Session not found for client %s", req.ClientID)
	}
	return sess.ReleaseLock(req.Filepath)
}

// DeleteLock removes the lock associated with the given file path from the session.
func (h *Handler) DeleteLock(req api.DeleteLockRequest, res *api.DeleteLockResponse) error {
	sess, ok := app.sessions[req.ClientID]
	if !ok {
		return fmt.Errorf("Session not found for client %s", req.ClientID)
	}
	return sess.DeleteLock(req.Filepath)
}

// ReadContent returns the contents of the file specified by the client.
func (h *Handler) ReadContent(req api.ReadRequest, res *api.ReadResponse) error {
	sess, ok := app.sessions[req.ClientID]
	if !ok {
		return fmt.Errorf("Session not found for client %s", req.ClientID)
	}
	content, err := sess.ReadContent(req.Filepath)
	if err != nil {
		return err
	}
	res.Content = content
	return nil
}

// WriteContent writes the provided content to the specified file for the client.
// Returns whether the operation was successful.
func (h *Handler) WriteContent(req api.WriteRequest, res *api.WriteResponse) error {
	sess, ok := app.sessions[req.ClientID]
	if !ok {
		return fmt.Errorf("Session not found for client %s", req.ClientID)
	}
	if err := sess.WriteContent(req.Filepath, req.Content); err != nil {
		res.IsSuccessful = false
		return err
	}
	res.IsSuccessful = true
	return nil
}
