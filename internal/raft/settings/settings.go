package settings

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

type Config struct {
	NodeID                int          `yaml:"node_id"`
	Hostname              string       `yaml:"hostname"`
	Port                  int          `yaml:"port"`
	ElectionTimeoutBaseMs int          `yaml:"election_timeout_base_ms"`
	DataPathTemplate      string       `yaml:"data_path_template"`
	Peers                 []PeerConfig `yaml:"peers"`
}

type PeerConfig struct {
	ServerID int    `yaml:"server_id"`
	Hostname string `yaml:"hostname"`
	Port     int    `yaml:"port"`
}

func LoadConfigFromFile(path string) (Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("read config file: %w", err)
	}

	var config Config
	if err := yaml.Unmarshal(data, &config); err != nil {
		return Config{}, fmt.Errorf("parse config file: %w", err)
	}

	return config, nil
}

func (c *Config) GetDataPath() string {
	return fmt.Sprintf(c.DataPathTemplate, c.NodeID)
}