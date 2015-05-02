package kveer

import "testing"

func TestMemorySyncClose(t *testing.T) {
	kv := NewMemory()
	// Ensure that Sync() returns a channel with nil error
	if err := <-kv.Sync(); err != nil {
		t.Error(err)
	}
	// Ensure that Close() return nil as an error
	if err := kv.Close(); err != nil {
		t.Error(err)
	}
}
