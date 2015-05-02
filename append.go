package kveer

import (
	"encoding/csv"
	"io"
	"os"
)

type record struct {
	Key   string
	Value []byte
	Err   chan error
}

type kvAppend struct {
	*kvMem
	w     io.WriteCloser
	queue chan *record
}

func NewAppend(path string) *kvAppend {
	kv := &kvAppend{
		kvMem: NewMemory(),
		queue: make(chan *record),
	}

	var lastErr error

	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		lastErr = err
	} else {
		r := csv.NewReader(f)
		for {
			rec, err := r.Read()
			if err != nil {
				if err != io.EOF {
					lastErr = err
				}
				break
			}
			kv.kvMem.m[rec[0]] = []byte(rec[1])
		}
	}

	var w *csv.Writer
	if lastErr == nil {
		w = csv.NewWriter(f)
	}

	go func() {
		for r := range kv.queue {
			if lastErr == nil {
				var rec []string
				if r.Value != nil {
					rec = []string{r.Key, string(r.Value)}
				} else {
					rec = []string{r.Key}
				}
				if err = w.Write(rec); err != nil {
					lastErr = err
				}
				if r != nil && r.Err != nil {
					w.Flush()
					f.Sync()
					r.Err <- nil
					close(r.Err)
				}
			} else if r.Err != nil {
				r.Err <- lastErr
			}
		}
		if f != nil {
			f.Close()
		}
	}()

	return kv
}

func (kv *kvAppend) Set(k string, v []byte) {
	kv.kvMem.Set(k, v)
	kv.queue <- &record{k, v, nil}
}

func (kv *kvAppend) Get(k string) []byte {
	return kv.kvMem.Get(k)
}

func (kv *kvAppend) Sync() <-chan error {
	c := make(chan error, 1)
	kv.queue <- &record{Err: c}
	return c
}

func (kv *kvAppend) Close() error {
	err := <-kv.Sync()
	close(kv.queue)
	return err
}
