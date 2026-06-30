package persistent

import (
	"encoding/json"
	"os"
	"path/filepath"

	"go.uber.org/zap"
)

type SimpleFilePersistent struct {
	Path   string
	logger zap.Logger
}

func (s *SimpleFilePersistent) UpdateState(state NodeState) error {
	tempFileName := filepath.Base(s.Path) + ".*"
	tempFile, err := os.CreateTemp(filepath.Dir(s.Path), tempFileName)
	if err != nil {
		s.logger.Error("failed to create temp file", zap.String("filename", tempFileName))
		return err
	}
	defer tempFile.Close()
	defer os.Remove(tempFile.Name())

	encoder := json.NewEncoder(tempFile)
	err = encoder.Encode(state)
	if err != nil {
		s.logger.Error("failed to write state to file", zap.String("path", tempFile.Name()))
		return err
	}

	err = os.Rename(tempFile.Name(), s.Path)
	if err != nil {
		s.logger.Error("failed to write new persistent state")
		return err
	}
	return err
}

func (s *SimpleFilePersistent) GetState(state NodeState) (*NodeState, error) {
	data, err := os.ReadFile(s.Path)
	if err != nil {
		s.logger.Error("failed to read file", zap.Error(err), zap.String("path", s.Path))
		return nil, err
	}
	var currState NodeState
	err = json.Unmarshal(data, &currState)
	if err != nil {
		s.logger.Error("failed to unmarshal node state", zap.Error(err), zap.String("content", string(data)))
		return nil, err
	}
	return &currState, err
}
