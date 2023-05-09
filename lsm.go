package main

import (
	"os"
	"sort"
	"strings"
)

func FindLSM(key string) (string, error) {
	dir, err := os.Open("./")
	if err != nil {
		return "", err
	}

	indexFiles := GetFilenamesByPredicate(dir, IsIndexFile)

	dir.Close()

	sort.Sort(sort.Reverse(sort.StringSlice(indexFiles)))

	for _, indexFilePath := range indexFiles {
		dataFilePath := strings.Replace(indexFilePath, "index-", "data-", 1)

		dataEntry, err := Find(key, indexFilePath, dataFilePath)
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
