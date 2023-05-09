package main

import (
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/huandu/skiplist"
	"os"
	"sync"
	"time"
)

type MemtableEntry struct {
	value     string
	tombstone bool
	timestamp uint64
}

type Memtable struct {
	currentMemtable     *skiplist.SkipList
	readOnlyMemtable    *skiplist.SkipList
	currentMemtableLock sync.RWMutex
	flushLock           sync.Mutex
}

func (m *Memtable) Get(key string) (string, error) {
	m.flushLock.Lock()
	m.currentMemtableLock.RLock()

	defer m.currentMemtableLock.RUnlock()
	defer m.flushLock.Unlock()

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

func (m *Memtable) Put(key string, value string, timestamp int64) {
	m.flushLock.Lock()
	m.currentMemtableLock.Lock()

	entry := MemtableEntry{
		value:     value,
		tombstone: false,
		timestamp: uint64(timestamp),
	}

	m.currentMemtable.Set(key, entry)

	m.flushLock.Unlock()
	m.currentMemtableLock.Unlock()
}

func (m *Memtable) Delete(key string, timestamp int64) {
	m.flushLock.Lock()
	m.currentMemtableLock.Lock()

	deletedEntry := MemtableEntry{
		value:     "",
		tombstone: true,
		timestamp: uint64(timestamp),
	}

	m.currentMemtable.Set(key, deletedEntry)

	m.flushLock.Unlock()
	m.currentMemtableLock.Unlock()
}

func (m *Memtable) MemtableSize() int {
	return m.currentMemtable.Len()
}

func (m *Memtable) TriggerBackgroundFlush() {
	m.flushLock.Lock()

	m.readOnlyMemtable = m.currentMemtable
	m.currentMemtable = skiplist.New(skiplist.StringAsc)

	FlushToFile(m.readOnlyMemtable)

	m.flushLock.Unlock()
}

func FlushToFile(s *skiplist.SkipList) {
	timestamp := time.Now().UnixNano()

	indexFilePath := fmt.Sprintf("index-%d.bin", timestamp)
	dataFilePath := fmt.Sprintf("data-%d.bin", timestamp)

	indexFilePathTemp := fmt.Sprintf("%s.temp", indexFilePath)
	dataFilePathTemp := fmt.Sprintf("%s.temp", dataFilePath)

	el := s.Front()

	indexFile, err := os.Create(indexFilePathTemp)
	if err != nil {
		panic(err)
	}

	dataFile, err := os.Create(dataFilePathTemp)
	if err != nil {
		panic(err)
	}

	dataOffset := 0
	indexOffset := 0

	for el != nil {
		strKey := el.Key().(string)
		val := el.Value.(MemtableEntry)

		binKey := []byte(strKey)
		binKeySize := binary.Size(binKey)

		binValue := []byte(val.value)
		binValueSize := binary.Size(binValue)

		data := DataEntry{
			keySize:   uint32(binKeySize),
			valueSize: uint32(binValueSize),
			timestamp: val.timestamp,
			tombstone: val.tombstone,
			key:       strKey,
			value:     val.value,
		}

		index := IndexEntry{
			keySize:    uint32(binKeySize),
			key:        strKey,
			dataOffset: uint32(dataOffset),
		}

		WriteDataRow(dataFile, &data)
		WriteIndexRow(indexFile, &index)

		dataOffset += data.BinarySize()
		indexOffset += index.BinarySize()

		el = el.Next()
	}

	indexFile.Close()
	dataFile.Close()

	os.Rename(indexFilePathTemp, indexFilePath)
	os.Rename(dataFilePathTemp, dataFilePath)
}
