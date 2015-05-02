package kveer

// TODO: compression
// TODO: sync policy: timeout or number of unsaved writes

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"

	"github.com/zserge/atomicwriter"
)

type kvFile struct {
	*kvMem
	path string
	sync chan chan error
}

func NewFile(path string) *kvFile {
	kv := &kvFile{
		kvMem: NewMemory(),
		path:  path,
		sync:  make(chan chan error),
	}

	err := kv.load() // no mutexes here, reading is single-threaded

	go func() {
		for c := range kv.sync {
			if err == nil {
				err = kv.save()
			}
			c <- err
			close(c)
		}
	}()
	return kv
}

func (kv *kvFile) load() error {
	f, err := os.Open(kv.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		} else {
			return err
		}
	}
	defer f.Close()
	r := csv.NewReader(f)
	for {
		if rec, err := r.Read(); err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		} else if len(rec) != 2 {
			return fmt.Errorf("Unexpected record: %v\n", rec)
		} else {
			kv.kvMem.m[rec[0]] = []byte(rec[1])
		}
	}
}

func (kv *kvFile) save() (err error) {
	f, err := atomicwriter.NewWriter(kv.path)
	if err != nil {
		return err
	}
	defer func() {
		err = f.Close()
	}()
	w := csv.NewWriter(f)
	defer w.Flush()

	kv.kvMem.Lock()
	defer kv.kvMem.Unlock()
	for k, v := range kv.kvMem.m {
		if err := w.Write([]string{k, string(v)}); err != nil {
			return err
		}
	}
	return nil
}

func (kv *kvFile) Set(k string, v []byte) {
	kv.kvMem.Set(k, v)
}

func (kv *kvFile) Get(k string) []byte {
	return kv.kvMem.Get(k)
}

func (kv *kvFile) Sync() <-chan error {
	c := make(chan error, 1)
	kv.sync <- c
	return c
}

func (kv *kvFile) Close() error {
	err := <-kv.Sync()
	close(kv.sync)
	return err
}
