package main

import (
	"encoding/binary"
	"fmt"
)

type DataEntry struct {
	keySize   uint32
	valueSize uint32
	timestamp uint64
	tombstone bool
	key       string
	value     string
}

func (d *DataEntry) BinarySize() int {
	binKey := []byte(d.key)
	binKeySize := binary.Size(binKey)

	binValue := []byte(d.value)
	binValueSize := binary.Size(binValue)

	return binary.Size(d.keySize) + binary.Size(d.valueSize) + binary.Size(d.timestamp) + binary.Size(d.tombstone) + int(binKeySize) + int(binValueSize)
}

func (d *DataEntry) String() string {
	if d == nil {
		return "nil DataEntry"
	}

	return fmt.Sprintf("DataEntry[keySize: %d, valueSize: %d, timestamp: %d, tombstone: %t, key: %s, value: %s]", d.keySize, d.valueSize, d.timestamp, d.tombstone, d.key, d.value)
}
