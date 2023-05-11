package main

import (
	"fmt"
	"hash/fnv"
	"os"
	"runtime"
)

var engine *Engine
var cores int

type Engine struct {
	shards []Shard
}

func InitializeEngine() {
	cores = runtime.NumCPU()

	shards := make([]Shard, cores)

	for i := 0; i < cores; i++ {
		shards[i] = NewShard(i)

		shardFolderPath := fmt.Sprintf("shard-%d", i)

		//TODO: proper perm
		os.Mkdir(shardFolderPath, os.ModePerm)
	}

	engine = &Engine{
		shards: shards,
	}
}

func (e *Engine) GetValue(key string) (string, error) {
	h := fnv.New32a()
	h.Write([]byte(key))

	shardId := int(h.Sum32()) % cores

	return e.shards[shardId].GetValue(key)
}

func (e *Engine) PutValue(key string, value string) error {
	h := fnv.New32a()
	h.Write([]byte(key))

	shardId := int(h.Sum32()) % cores

	return e.shards[shardId].PutValue(key, value)
}

func (e *Engine) DeleteValue(key string) error {
	h := fnv.New32a()
	h.Write([]byte(key))

	shardId := int(h.Sum32()) % cores

	return e.shards[shardId].DeleteValue(key)
}
