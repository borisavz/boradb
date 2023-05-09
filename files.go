package main

import (
	"encoding/binary"
	"os"
)

func WriteIndexRow(indexFile *os.File, indexEntry *IndexEntry) {
	binKey := []byte(indexEntry.key)

	binary.Write(indexFile, binary.BigEndian, indexEntry.keySize)
	binary.Write(indexFile, binary.BigEndian, binKey)
	binary.Write(indexFile, binary.BigEndian, indexEntry.dataOffset)
}

func WriteDataRow(dataFile *os.File, dataEntry *DataEntry) {
	binKey := []byte(dataEntry.key)

	binary.Write(dataFile, binary.BigEndian, dataEntry.keySize)
	binary.Write(dataFile, binary.BigEndian, dataEntry.valueSize)
	binary.Write(dataFile, binary.BigEndian, dataEntry.timestamp)
	binary.Write(dataFile, binary.BigEndian, dataEntry.tombstone)
	binary.Write(dataFile, binary.BigEndian, binKey)
	binary.Write(dataFile, binary.BigEndian, dataEntry.value)
}
