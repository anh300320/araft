package common

import (
	"errors"
	"os"
)

func GetMajorityCount(total int) int {
	return total/2 + 1
}

func FileExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	return false, err
}
