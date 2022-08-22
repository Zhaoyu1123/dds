package bucket

import "context"

// Bucket is a storage object.
type Bucket interface {
	// Add value to bucket.
	Add(ctx context.Context, value string) error

	// Flush used for process callback for each value in bucket.
	Flush(callback func(value string))

	// Expiration return the bucket expiration timestamp.
	Expiration() int64

	// SetExpiration set bucket expiration timestamp.
	SetExpiration(expiration int64) bool

	// Index is uniquely identifies of bucket.
	Index() string
}
