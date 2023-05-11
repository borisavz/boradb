package main

import (
	"encoding/gob"
	"fmt"
	"github.com/bits-and-blooms/bloom/v3"
	"os"
	"sort"
	"strings"
)

func FindLSM(shardId int, key string) (string, error) {
	shardPath := fmt.Sprintf("./shard-%d", shardId)

	dir, err := os.Open(shardPath)
	if err != nil {
		return "", err
	}

	indexFiles := GetFilenamesByPredicate(dir, IsIndexFile)

	dir.Close()

	sort.Sort(sort.Reverse(sort.StringSlice(indexFiles)))

	for _, indexFilePath := range indexFiles {
		filterFilePath := strings.Replace(indexFilePath, "index-", "filter-", 1)

		filterFile, err := os.Open(shardPath + "/" + filterFilePath)
		if err != nil {
			return "", err
		}

		filter := new(bloom.BloomFilter)

		decoder := gob.NewDecoder(filterFile)

		err = decoder.Decode(filter)
		if err != nil {
			break
		}

		filterFile.Close()

		if !filter.Test([]byte(key)) {
			continue
		}

		dataFilePath := strings.Replace(indexFilePath, "index-", "data-", 1)

		dataEntry, err := Find(key, shardPath+"/"+indexFilePath, shardPath+"/"+dataFilePath)
		if err != nil {

		}

		if dataEntry == nil {
			continue
		}

		if dataEntry.tombstone {
			return "deleted", nil
		}

		return dataEntry.value, nil
	}

	return "not found", nil
}
