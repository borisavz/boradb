package main

import (
	"github.com/huandu/skiplist"
)

type Engine struct {
	memtable *Memtable
}

func InitializeEngine() *Engine {
	m := Memtable{
		currentMemtable:  skiplist.New(skiplist.StringAsc),
		readOnlyMemtable: skiplist.New(skiplist.StringAsc),
	}

	return &Engine{
		memtable: &m,
	}
}

func (e *Engine) GetValue(key string) (string, error) {
	val, err := e.memtable.Get(key)

	if err != nil {
		return FindLSM(key)
	}

	return val, nil
}

func (e *Engine) PutValue(key string, value string) error {
	e.memtable.Put(key, value)

	if e.memtable.MemtableSize() == 3 {
		go e.memtable.TriggerBackgroundFlush()
	}

	return nil
}

func (e *Engine) DeleteValue(key string) error {
	e.memtable.Delete(key)

	return nil
}
