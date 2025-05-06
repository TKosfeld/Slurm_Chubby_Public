// api/api.go
package api

import "time"

// ClientID represents a unique client identifier.
type ClientID string

// FilePath represents the path to a lock/file.
type FilePath string

// LockMode represents the mode of a lock.
type LockMode string



const (
	FREE      LockMode = ""
	SHARED    LockMode = "SHARED"
	EXCLUSIVE LockMode = "EXCLUSIVE"
)

// InitSessionRequest ...
type InitSessionRequest struct {
	ClientID ClientID
}

// InitSessionResponse ...
type InitSessionResponse struct {
	Error error
}

// KeepAliveRequest ...
type KeepAliveRequest struct {
	ClientID ClientID
	Locks     map[FilePath]LockMode
}

// KeepAliveResponse ...
type KeepAliveResponse struct {
	LeaseLength time.Duration
}

// OpenLockRequest ...
type OpenLockRequest struct {
	ClientID ClientID
	Filepath FilePath
}

// OpenLockResponse ...
type OpenLockResponse struct {
	Error error
}

// DeleteLockRequest ...
type DeleteLockRequest struct {
	ClientID ClientID
	Filepath FilePath
}

// DeleteLockResponse ...
type DeleteLockResponse struct {
	Error error
}

// TryAcquireLockRequest ...
type TryAcquireLockRequest struct {
	ClientID ClientID
	Filepath FilePath
	Mode      LockMode
}

// TryAcquireLockResponse ...
type TryAcquireLockResponse struct {
	IsSuccessful bool
	Error         error
}

// ReleaseLockRequest ...
type ReleaseLockRequest struct {
	ClientID ClientID
	Filepath FilePath
}

// ReleaseLockResponse ...
type ReleaseLockResponse struct {
	Error error
}

// ReadRequest ...
type ReadRequest struct {
	ClientID ClientID // ADDED THIS LINE
	Filepath FilePath
}

// ReadResponse ...
type ReadResponse struct {
	Content string
	Error   error
}

// WriteRequest ...
type WriteRequest struct {
	ClientID ClientID
	Filepath FilePath
	Content  string
}

// WriteResponse ...
type WriteResponse struct {
	IsSuccessful bool
	Error         error
}

type RegisterSessionRequest struct {
	ClientID ClientID
}

type RegisterSessionResponse struct{}

