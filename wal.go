package main

import (
	"encoding/binary"
	"os"
	"sync"
)

type WriteAheadLog struct {
	writeLock sync.Mutex
}

func (w *WriteAheadLog) Append(key string, value string, timestamp int64, tombstone bool) {
	w.writeLock.Lock()
	defer w.writeLock.Unlock()

	binKey := []byte(key)
	binKeySize := binary.Size(binKey)

	binValue := []byte(value)
	binValueSize := binary.Size(binValue)

	walEntry := WALEntry{
		keySize:   uint32(binKeySize),
		valueSize: uint32(binValueSize),
		timestamp: uint64(timestamp),
		tombstone: tombstone,
		key:       key,
		value:     value,
	}

	//TODO: write using O_DIRECT to skip fs cache
	walFile, err := os.OpenFile("wal-current.bin", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		//TODO: handle error
	}

	WriteWALRow(walFile, &walEntry)
}

func Recover() {
	walCurrent, err := os.Open("wal-current.bin")
	if err != nil {
		return
	}

	for {
		walEntry := ReadWALRow(walCurrent)

		if walEntry == nil {
			break
		}

		println(walEntry.String())
	}
}
