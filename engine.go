package main

import (
	"github.com/huandu/skiplist"
	"time"
)

var engine *Engine

type Engine struct {
	memtable *Memtable
	wal      *WriteAheadLog
}

func InitializeEngine() {
	m := Memtable{
		currentMemtable:  skiplist.New(skiplist.StringAsc),
		readOnlyMemtable: skiplist.New(skiplist.StringAsc),
	}

	w := WriteAheadLog{}

	engine = &Engine{
		memtable: &m,
		wal:      &w,
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
	timestamp := time.Now().UnixNano()

	e.memtable.Put(key, value, timestamp)
	e.wal.Append(key, value, timestamp, false)

	//if e.memtable.MemtableSize() == 3 {
	//	go e.memtable.TriggerBackgroundFlush()
	//}

	return nil
}

func (e *Engine) DeleteValue(key string) error {
	timestamp := time.Now().UnixNano()

	e.memtable.Delete(key, timestamp)
	e.wal.Append(key, "", timestamp, true)

	return nil
}
