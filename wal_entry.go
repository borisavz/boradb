package main

import (
	"encoding/binary"
	"fmt"
)

type WALEntry struct {
	keySize   uint32
	valueSize uint32
	timestamp uint64
	tombstone bool
	key       string
	value     string
}

func (w *WALEntry) BinarySize() int {
	binKey := []byte(w.key)
	binKeySize := binary.Size(binKey)

	binValue := []byte(w.value)
	binValueSize := binary.Size(binValue)

	return binary.Size(w.keySize) + binary.Size(w.valueSize) + binary.Size(w.timestamp) + binary.Size(w.tombstone) + int(binKeySize) + int(binValueSize)
}

func (w *WALEntry) String() string {
	if w == nil {
		return "nil WALEntry"
	}

	return fmt.Sprintf("WALEntry[keySize: %w, valueSize: %w, timestamp: %w, tombstone: %t, key: %s, value: %s]", w.keySize, w.valueSize, w.timestamp, w.tombstone, w.key, w.value)
}
