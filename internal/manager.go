package internal

import (
	"github.com/anh300320/araft/internal/raft"
	"github.com/anh300320/araft/internal/raft/settings"
	"github.com/anh300320/araft/internal/raft/states"
	"go.uber.org/zap"
)

func NewRaftNode(logger *zap.Logger, configPath string) *raft.Raft {
	appConfig, err := settings.LoadConfigFromFile(configPath)
	if err != nil {
		logger.Fatal("failed to load config", zap.Error(err))
	}

	newNode := raft.NewRaftNode(logger, appConfig)
	follower := states.NewFollower(newNode, appConfig)
	return follower
}
