package main

import (
	"encoding/binary"
	"encoding/gob"
	"errors"
	"fmt"
	"github.com/bits-and-blooms/bloom/v3"
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
	shardId            int
	currentMemtable    *skiplist.SkipList
	readOnlyMemtable   *skiplist.SkipList
	memtableSwitchLock sync.Mutex
	flushLock          sync.Mutex
}

func (m *Memtable) Get(key string) (string, error) {
	m.memtableSwitchLock.Lock()
	defer m.memtableSwitchLock.Unlock()

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
	m.memtableSwitchLock.Lock()
	defer m.memtableSwitchLock.Unlock()

	entry := MemtableEntry{
		value:     value,
		tombstone: false,
		timestamp: uint64(timestamp),
	}

	m.currentMemtable.Set(key, entry)
}

func (m *Memtable) Delete(key string, timestamp int64) {
	m.memtableSwitchLock.Lock()
	defer m.memtableSwitchLock.Unlock()

	deletedEntry := MemtableEntry{
		value:     "",
		tombstone: true,
		timestamp: uint64(timestamp),
	}

	m.currentMemtable.Set(key, deletedEntry)
}

func (m *Memtable) MemtableSize() int {
	m.memtableSwitchLock.Lock()
	defer m.memtableSwitchLock.Unlock()

	return m.currentMemtable.Len()
}

func (m *Memtable) TriggerBackgroundFlush() {
	m.flushLock.Lock()
	defer m.flushLock.Unlock()

	m.memtableSwitchLock.Lock()

	m.readOnlyMemtable = m.currentMemtable
	m.currentMemtable = skiplist.New(skiplist.StringAsc)

	timestamp := time.Now().UnixNano()

	m.memtableSwitchLock.Unlock()

	FlushToFile(m.shardId, timestamp, m.readOnlyMemtable)
}

func FlushToFile(shardId int, timestamp int64, s *skiplist.SkipList) {
	filterFilePath := fmt.Sprintf("shard-%d/filter-%d.bin", shardId, timestamp)
	indexFilePath := fmt.Sprintf("shard-%d/index-%d.bin", shardId, timestamp)
	dataFilePath := fmt.Sprintf("shard-%d/data-%d.bin", shardId, timestamp)
	walFilePath := fmt.Sprintf("shard-%d/wal-%d.bin", shardId, timestamp)

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

	filter := bloom.NewWithEstimates(1000, 0.01)

	for el != nil {
		strKey := el.Key().(string)
		val := el.Value.(MemtableEntry)

		filter.Add([]byte(strKey))

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

	filterFile, err := os.Create(filterFilePath)
	encoder := gob.NewEncoder(filterFile)

	err = encoder.Encode(filter)
	if err != nil {
		fmt.Println(err)
	}

	os.Rename(indexFilePathTemp, indexFilePath)
	os.Rename(dataFilePathTemp, dataFilePath)

	println(walFilePath)
	os.Remove(walFilePath)
}
