package main

import (
	"encoding/binary"
	"fmt"
	"github.com/huandu/skiplist"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type WriteAheadLog struct {
	writeLock sync.Mutex
}

func (w *WriteAheadLog) RenameCurrentWAL(newName string) {
	w.writeLock.Lock()
	defer w.writeLock.Unlock()

	os.Rename("wal-current.bin", newName)
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
	timestamp := time.Now().UnixNano()
	walFilePath := fmt.Sprintf("wal-%d.bin", timestamp)

	os.Rename("wal-current.bin", walFilePath)

	dir, err := os.Open("./")
	if err != nil {
		//TODO: handle error
		return
	}

	walFiles := GetFilenamesByPredicate(dir, IsWALFile)

	dir.Close()

	for _, f := range walFiles {
		timestampString := strings.ReplaceAll(strings.ReplaceAll(f, "wal-", ""), ".bin", "")
		walTimestamp, err := strconv.Atoi(timestampString)

		walCurrent, err := os.Open(f)
		if err != nil {
			return
		}

		memtable := skiplist.New(skiplist.StringAsc)

		for {
			walEntry := ReadWALRow(walCurrent)

			if walEntry != nil {
				m := MemtableEntry{
					value:     walEntry.value,
					tombstone: walEntry.tombstone,
					timestamp: walEntry.timestamp,
				}

				memtable.Set(walEntry.key, m)
			} else {
				break
			}
		}

		walCurrent.Close()

		FlushToFile(int64(walTimestamp), memtable)
	}
}
