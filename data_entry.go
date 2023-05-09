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
	value     []byte
}

func (d *DataEntry) BinarySize() int {
	binKey := []byte(d.key)
	binKeySize := binary.Size(binKey)

	return binary.Size(d.keySize) + binary.Size(d.valueSize) + binary.Size(d.timestamp) + binary.Size(d.tombstone) + int(binKeySize) + int(d.valueSize)
}

func (d *DataEntry) String() string {
	return fmt.Sprintf("DataEntry[keySize: %d, valueSize: %d, timestamp: %d, tombstone: %t, key: %s, value: ???]", d.keySize, d.valueSize, d.timestamp, d.tombstone, d.key)
}
