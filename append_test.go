package kveer

// TODO test remove, save, read

import (
	"os"
	"syscall"
	"testing"
)

func TestAppendOverwrite(t *testing.T) {
	kv := NewAppend(KV_APPENDFILE)
	kv.Set("a", []byte("foo"))
	kv.Set("a", []byte("bar"))
	kv.Set("a", []byte("baz"))
	kv.Close()

	syscall.Sync()

	kv = NewAppend(KV_APPENDFILE)
	if a := kv.Get("a"); string(a) != "baz" {
		t.Error(a)
	}

	os.Remove(KV_APPENDFILE)
}

func TestAppendReader(t *testing.T) {
	kv := NewAppend(KV_APPENDFILE)
	kv.Set("a", []byte("foo"))
	if err := <-kv.Sync(); err != nil {
		t.Error(err)
	}
	syscall.Sync()

	r := NewAppend(KV_APPENDFILE)
	if a := r.Get("a"); string(a) != "foo" {
		t.Error(a)
	}
	r.Close()

	kv.Set("a", []byte("bar"))
	if err := <-kv.Sync(); err != nil {
		t.Error(err)
	}
	syscall.Sync()

	r = NewAppend(KV_APPENDFILE)
	if a := r.Get("a"); string(a) != "bar" {
		t.Error(a)
	}
	r.Close()

	kv.Close()
	os.Remove(KV_APPENDFILE)
}

func TestAppendError(t *testing.T) {
	// This filename is not allowed on most systems, so will result in an error
	kv := NewAppend("some\x00invalid\x00file")
	kv.Set("a", []byte("foo"))
	if err := <-kv.Sync(); err == nil {
		t.Error()
	}
	if err := kv.Close(); err == nil {
		t.Error()
	}
}
