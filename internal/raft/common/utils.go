package common

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"path"
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

func BuildURL(host string, endpoint string) (string, error) {
	hostURL, err := url.Parse(host)
	if err != nil {
		return "", fmt.Errorf("failed to build host URL: %w", err)
	}
	hostURL.Path = path.Join(hostURL.Path, endpoint)
	return hostURL.String(), nil
}
