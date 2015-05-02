package kveer

// TODO: test non-printable chars

import (
	"os"
	"sort"
	"testing"
)

const KV_APPENDFILE = "append.csv"
const KV_FILE = "file.csv"

var KVs = []func() KV{
	func() KV { return NewMemory() },
	func() KV { return NewFile(KV_FILE) },
	func() KV { return NewAppend(KV_APPENDFILE) },
}

func TestKveerSetGet(t *testing.T) {
	for _, builder := range KVs {
		kv := builder()
		kv.Set("a", []byte("foo"))
		kv.Set("b", []byte("bar"))
		if a := kv.Get("a"); string(a) != "foo" {
			t.Error(a)
		}
		if a := kv.Get("b"); string(a) != "bar" {
			t.Error(a)
		}
		kv.Set("a", []byte("baz"))
		if a := kv.Get("a"); string(a) != "baz" {
			t.Error(a)
		}
		kv.Close()
	}
	os.Remove(KV_FILE)
	os.Remove(KV_APPENDFILE)
}

func TestKveerKeys(t *testing.T) {
	for _, builder := range KVs {
		kv := builder()
		kv.Set("user:john", []byte("John Doe"))
		kv.Set("user:jane", []byte("Jane Doe"))
		kv.Set("users:count", []byte("2"))

		keys := kv.Keys("user:")
		sort.Strings(keys)

		if len(keys) != 2 || keys[0] != "user:jane" || keys[1] != "user:john" {
			t.Error(keys)
		}
		kv.Close()
	}
	os.Remove(KV_FILE)
	os.Remove(KV_APPENDFILE)
}

func TestKveerDelete(t *testing.T) {
	for _, builder := range KVs {
		kv := builder()
		kv.Set("a", []byte("foo"))
		if a := kv.Get("a"); string(a) != "foo" {
			t.Error(a)
		}
		kv.Set("a", nil)
		if a := kv.Get("a"); a != nil {
			t.Error(a)
		}
		keys := kv.Keys("")
		if len(keys) != 0 {
			t.Error(keys)
		}

		kv.Close()
	}
	os.Remove(KV_FILE)
	os.Remove(KV_APPENDFILE)
}
