package kveer

import (
	"os"
	"syscall"
	"testing"
)

func TestFileSync(t *testing.T) {
	// Create store, put a value and save
	kv1 := NewFile(KV_FILE)
	kv1.Set("a", []byte("foo"))
	kv1.Close()

	// Create another store, read a value, check
	kv2 := NewFile(KV_FILE)
	if a := kv2.Get("a"); string(a) != "foo" {
		t.Error(a)
	}
	kv2.Close()
	os.Remove(KV_FILE)
}

func TestFileParallel(t *testing.T) {
	kv1 := NewFile(KV_FILE)
	kv1.Set("a", []byte("foo"))
	kv1.Set("b", []byte("bar"))
	<-kv1.Sync()

	kv2 := NewFile(KV_FILE)
	if a := kv2.Get("a"); string(a) != "foo" {
		t.Error(a)
	}

	kv1.Set("a", []byte("baz"))
	<-kv1.Sync()

	syscall.Sync()

	// New reader should get new value
	kv3 := NewFile(KV_FILE)
	if a := kv3.Get("a"); string(a) != "baz" {
		t.Error(a)
	}
	kv3.Close()

	// Old reader shall still have the previous value
	if a := kv2.Get("a"); string(a) != "foo" {
		t.Error(a)
	}
	kv2.Close()

	// But new client should get the new value
	os.Remove(KV_FILE)
}

func TestFileError(t *testing.T) {
	// This filename is not allowed on most systems, so will result in an error
	kv := NewFile("some\x00invalid\x00file")
	kv.Set("a", []byte("foo"))
	if err := <-kv.Sync(); err == nil {
		t.Error()
	}
	if err := kv.Close(); err == nil {
		t.Error()
	}
}
