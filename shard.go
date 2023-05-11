package main

import (
	"fmt"
	"github.com/huandu/skiplist"
	"sync"
	"time"
)

type Shard struct {
	id        int
	memtable  *Memtable
	wal       *WriteAheadLog
	shardLock sync.Mutex
}

func NewShard(id int) Shard {
	m := Memtable{
		shardId:          id,
		currentMemtable:  skiplist.New(skiplist.StringAsc),
		readOnlyMemtable: skiplist.New(skiplist.StringAsc),
	}

	w := WriteAheadLog{
		shardId: id,
	}

	return Shard{
		id:       id,
		memtable: &m,
		wal:      &w,
	}
}

func (s *Shard) GetValue(key string) (string, error) {
	s.shardLock.Lock()
	defer s.shardLock.Unlock()

	val, err := s.memtable.Get(key)

	if err != nil {
		return FindLSM(s.id, key)
	}

	return val, nil
}

func (s *Shard) PutValue(key string, value string) error {
	s.shardLock.Lock()
	defer s.shardLock.Unlock()

	timestamp := time.Now().UnixNano()

	s.memtable.Put(key, value, timestamp)
	s.wal.Append(key, value, timestamp, false)

	if s.memtable.MemtableSize() == 3 {
		//TODO: check if sync working properly
		walName := fmt.Sprintf("wal-%d.bin", timestamp)

		s.wal.RenameCurrentWAL(walName)
		go s.memtable.TriggerBackgroundFlush()
	}

	return nil
}

func (s *Shard) DeleteValue(key string) error {
	s.shardLock.Lock()
	defer s.shardLock.Unlock()

	timestamp := time.Now().UnixNano()

	s.memtable.Delete(key, timestamp)
	s.wal.Append(key, "", timestamp, true)

	return nil
}
