package kveer

//
// KV is a generic interface for a simple key-value store.
// The store keys are strings, store values are []byte slices.
// Store keys are unsorted. If store is persistent - its persistance is
// guaranteed after the Close() call is finished without errors.
// Alternatively, one may call Sync() and read from the channel to get the sync
// error.
// Get() and Set() copy byte slices, so for the store they are treated as
// immutable
//
type KV interface {
	Set(k string, v []byte)
	Get(k string) []byte
	Keys(prefix string) []string
	Sync() <-chan error
	Close() error
}
