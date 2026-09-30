package main

import (
	"flag"

	"github.com/anh300320/araft/internal"
	"go.uber.org/zap"
)

func main() {
	configPath := flag.String("config", "config.yaml", "path to raft config file")
	flag.Parse()

	log, err := zap.NewProduction()
	if err != nil {
		panic(err)
	}
	defer log.Sync()

	raftNode := internal.NewRaftNode(log, *configPath)
	raftNode.Run()
}
