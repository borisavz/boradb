package main

import (
	"encoding/binary"
	"fmt"
)

type IndexEntry struct {
	keySize    uint32
	key        string
	dataOffset uint32
}

func (i *IndexEntry) BinarySize() int {
	binKey := []byte(i.key)
	binKeySize := binary.Size(binKey)

	return binary.Size(i.keySize) + binKeySize + binary.Size(i.dataOffset)
}

func (i *IndexEntry) String() string {
	return fmt.Sprintf("IndexEntry[keySize: %d, key: %s, dataOffset: %d]", i.keySize, i.key, i.dataOffset)
}
