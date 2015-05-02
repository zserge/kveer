package kveer

import (
	"strings"
	"sync"
)

var _ KV = &kvMem{}

//
// In-memory thread-safe key/value store
// TODO RLock
//
type kvMem struct {
	sync.RWMutex
	m map[string][]byte
}

func NewMemory() *kvMem {
	return &kvMem{m: map[string][]byte{}}
}

func (kv *kvMem) Set(k string, v []byte) {
	kv.Lock()
	defer kv.Unlock()
	if v != nil {
		kv.m[k] = v[:]
	} else {
		delete(kv.m, k)
	}
}

func (kv *kvMem) Get(k string) []byte {
	kv.RLock()
	defer kv.RUnlock()
	if v, ok := kv.m[k]; ok {
		return v[:]
	} else {
		return nil
	}
}

// Keys() returns list of keys (unsorted)
func (kv *kvMem) Keys(prefix string) []string {
	kv.RLock()
	defer kv.RUnlock()
	keys := []string{}
	for k, _ := range kv.m {
		if strings.HasPrefix(k, prefix) {
			keys = append(keys, k)
		}
	}
	return keys
}

// Sync() always returns a channel with one nil error written
func (kv *kvMem) Sync() <-chan error {
	c := make(chan error, 1)
	c <- nil
	close(c)
	return c
}

// Close() returns nil, since in-memory map produces no errors
func (kv *kvMem) Close() error {
	return nil
}
