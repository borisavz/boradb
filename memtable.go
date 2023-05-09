package main

import (
	"errors"
	"github.com/huandu/skiplist"
	"sync"
)

type Memtable struct {
	currentMemtable     *skiplist.SkipList
	readOnlyMemtable    *skiplist.SkipList
	currentMemtableLock sync.RWMutex
	compactionLock      sync.Mutex
}

func (m *Memtable) Get(key string) (string, error) {
	m.compactionLock.Lock()
	m.currentMemtableLock.RLock()

	defer m.currentMemtableLock.RUnlock()
	defer m.compactionLock.Unlock()

	val, ok := m.currentMemtable.GetValue(key)
	if !ok {
		val, ok = m.readOnlyMemtable.GetValue(key)
		if !ok {
			return "", errors.New("Key does not exist!")
		}

		entry := val.(MemtableEntry)

		if entry.tombstone {
			return "", errors.New("Key does not exist!")
		}

		return entry.value, nil
	}

	entry := val.(MemtableEntry)

	if entry.tombstone {
		return "", errors.New("Key does not exist!")
	}

	return entry.value, nil
}

func (m *Memtable) Put(key string, value string) {
	m.compactionLock.Lock()
	m.currentMemtableLock.Lock()

	entry := MemtableEntry{value, false}

	m.currentMemtable.Set(key, entry)

	m.compactionLock.Unlock()
	m.currentMemtableLock.Unlock()
}

func (m *Memtable) Delete(key string) {
	m.compactionLock.Lock()
	m.currentMemtableLock.Lock()

	deletedEntry := MemtableEntry{"", true}

	m.currentMemtable.Set(key, deletedEntry)

	m.compactionLock.Unlock()
	m.currentMemtableLock.Unlock()
}

func (m *Memtable) MemtableSize() int {
	return m.currentMemtable.Len()
}

func (m *Memtable) TriggerBackgroundCompaction() {
	m.compactionLock.Lock()

	m.readOnlyMemtable = m.currentMemtable
	m.currentMemtable = skiplist.New(skiplist.StringAsc)

	m.compactionLock.Unlock()
}
