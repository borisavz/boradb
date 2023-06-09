package main

import (
	"encoding/binary"
	"os"
	"strings"
)

func Find(searchKey string, indexFilePath string, dataFilePath string) (*DataEntry, error) {
	indexFile, err := os.Open(indexFilePath)
	if err != nil {
		panic(err)
	}

	dataFile, err := os.Open(dataFilePath)
	if err != nil {
		panic(err)
	}

	for {
		index := ReadIndexRow(indexFile)

		if index == nil {
			break
		}

		if index.key == searchKey {
			return ReadDataRowAtOffset(dataFile, int64(index.dataOffset)), nil
		}
	}

	indexFile.Close()
	dataFile.Close()

	return nil, nil
}

func ReadIndexRow(indexFile *os.File) *IndexEntry {
	keySizeBin := make([]byte, 4)
	err := binary.Read(indexFile, binary.BigEndian, keySizeBin)
	if err != nil {
		return nil
	}

	keySize := binary.BigEndian.Uint32(keySizeBin)

	keyBin := make([]byte, keySize)
	binary.Read(indexFile, binary.BigEndian, keyBin)

	key := string(keyBin)

	dataOffsetBin := make([]byte, 4)
	binary.Read(indexFile, binary.BigEndian, dataOffsetBin)

	dataOffset := binary.BigEndian.Uint32(dataOffsetBin)

	return &IndexEntry{
		keySize:    keySize,
		key:        key,
		dataOffset: dataOffset,
	}
}

func ReadDataRowAtOffset(dataFile *os.File, dataOffset int64) *DataEntry {
	dataFile.Seek(dataOffset, 0)

	dataKeySizeBin := make([]byte, 4)
	binary.Read(dataFile, binary.BigEndian, dataKeySizeBin)
	dataKeySize := binary.BigEndian.Uint32(dataKeySizeBin)

	dataValueSizeBin := make([]byte, 4)
	binary.Read(dataFile, binary.BigEndian, dataValueSizeBin)
	dataValueSize := binary.BigEndian.Uint32(dataValueSizeBin)

	dataTimestampBin := make([]byte, 8)
	binary.Read(dataFile, binary.BigEndian, dataTimestampBin)
	dataTimestamp := binary.BigEndian.Uint64(dataTimestampBin)

	dataTombstoneBin := make([]byte, 1)
	binary.Read(dataFile, binary.BigEndian, dataTombstoneBin)
	dataTombstone := false
	if dataTombstoneBin[0] == 1 {
		dataTombstone = true
	}

	dataKeyBin := make([]byte, dataKeySize)
	binary.Read(dataFile, binary.BigEndian, dataKeyBin)
	dataKey := string(dataKeyBin)

	dataValueBin := make([]byte, dataValueSize)
	dataValue := ""
	if dataValueSize != 0 {
		binary.Read(dataFile, binary.BigEndian, dataValueBin)
		dataValue = string(dataValueBin)
	}

	return &DataEntry{
		keySize:   dataKeySize,
		valueSize: dataValueSize,
		timestamp: dataTimestamp,
		tombstone: dataTombstone,
		key:       dataKey,
		value:     dataValue,
	}
}

func WriteIndexRow(indexFile *os.File, indexEntry *IndexEntry) {
	binKey := []byte(indexEntry.key)

	binary.Write(indexFile, binary.BigEndian, indexEntry.keySize)
	binary.Write(indexFile, binary.BigEndian, binKey)
	binary.Write(indexFile, binary.BigEndian, indexEntry.dataOffset)
}

func WriteDataRow(dataFile *os.File, dataEntry *DataEntry) {
	binKey := []byte(dataEntry.key)
	binValue := []byte(dataEntry.value)

	binary.Write(dataFile, binary.BigEndian, dataEntry.keySize)
	binary.Write(dataFile, binary.BigEndian, dataEntry.valueSize)
	binary.Write(dataFile, binary.BigEndian, dataEntry.timestamp)
	binary.Write(dataFile, binary.BigEndian, dataEntry.tombstone)
	binary.Write(dataFile, binary.BigEndian, binKey)
	binary.Write(dataFile, binary.BigEndian, binValue)
}

func ReadWALRow(dataFile *os.File) *WALEntry {
	dataKeySizeBin := make([]byte, 4)
	err := binary.Read(dataFile, binary.BigEndian, dataKeySizeBin)
	if err != nil {
		return nil
	}
	dataKeySize := binary.BigEndian.Uint32(dataKeySizeBin)

	dataValueSizeBin := make([]byte, 4)
	err = binary.Read(dataFile, binary.BigEndian, dataValueSizeBin)
	if err != nil {
		return nil
	}
	dataValueSize := binary.BigEndian.Uint32(dataValueSizeBin)

	dataTimestampBin := make([]byte, 8)
	err = binary.Read(dataFile, binary.BigEndian, dataTimestampBin)
	if err != nil {
		return nil
	}
	dataTimestamp := binary.BigEndian.Uint64(dataTimestampBin)

	dataTombstoneBin := make([]byte, 1)
	err = binary.Read(dataFile, binary.BigEndian, dataTombstoneBin)
	if err != nil {
		return nil
	}
	dataTombstone := false
	if dataTombstoneBin[0] == 1 {
		dataTombstone = true
	}

	dataKeyBin := make([]byte, dataKeySize)
	err = binary.Read(dataFile, binary.BigEndian, dataKeyBin)
	if err != nil {
		return nil
	}
	dataKey := string(dataKeyBin)

	dataValueBin := make([]byte, dataValueSize)
	dataValue := ""
	if dataValueSize != 0 {
		err = binary.Read(dataFile, binary.BigEndian, dataValueBin)
		if err != nil {
			return nil
		}
		dataValue = string(dataValueBin)
	}

	return &WALEntry{
		keySize:   dataKeySize,
		valueSize: dataValueSize,
		timestamp: dataTimestamp,
		tombstone: dataTombstone,
		key:       dataKey,
		value:     dataValue,
	}
}

func WriteWALRow(walFile *os.File, walEntry *WALEntry) {
	binKey := []byte(walEntry.key)
	binValue := []byte(walEntry.value)

	binary.Write(walFile, binary.BigEndian, walEntry.keySize)
	binary.Write(walFile, binary.BigEndian, walEntry.valueSize)
	binary.Write(walFile, binary.BigEndian, walEntry.timestamp)
	binary.Write(walFile, binary.BigEndian, walEntry.tombstone)
	binary.Write(walFile, binary.BigEndian, binKey)
	binary.Write(walFile, binary.BigEndian, binValue)
}

func GetFilenamesByPredicate(dir *os.File, predicate func(string) bool) []string {
	files := make([]string, 0)

	for {
		names, err := dir.Readdirnames(100)

		if err != nil {
			break
		}

		if len(names) == 0 {
			break
		}

		for _, n := range names {
			if predicate(n) {
				files = append(files, n)
			}
		}
	}

	return files
}

func IsIndexFile(name string) bool {
	return strings.HasPrefix(name, "index-") && strings.HasSuffix(name, ".bin")
}

func IsWALFile(name string) bool {
	return strings.HasPrefix(name, "wal-") && strings.HasSuffix(name, ".bin")
}
