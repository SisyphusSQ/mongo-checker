package config

import (
	"fmt"
	"os"
	"slices"
	"strings"

	"github.com/pelletier/go-toml"

	"mongo-checker/vars"
)

type Config struct {
	Source      string `toml:"source"`
	Destination string `toml:"destination"`
	ConnectMode string `toml:"connect_mode"`

	ExcludeDBs   string `toml:"exclude_dbs"`
	IncludeDBs   string `toml:"include_dbs"`
	ExcludeColls string `toml:"exclude_colls"`
	IncludeColls string `toml:"include_colls"`
	DBTrans      string `toml:"database_transform"`

	LimitQPS int `toml:"limit_qps"`
	Parallel int `toml:"parallel"`

	Debug   bool   `toml:"debug"`
	LogPath string `toml:"log_path"`
}

func NewConfig(configPath string) (*Config, error) {
	file, err := os.Open(configPath)
	defer file.Close()
	if err != nil {
		return nil, fmt.Errorf("failed to open config file, %s", err.Error())
	}
	decoder := toml.NewDecoder(file)
	c := new(Config)
	err = decoder.Decode(c)
	if err != nil {
		return nil, err
	}
	c.PreCheck()
	return c, nil
}

func (c *Config) PreCheck() {
	if c.Source == "" || c.Destination == "" {
		panic("source and destination must be set")
	}

	if c.ExcludeDBs != "" && c.IncludeDBs != "" {
		panic("exclude_dbs and include_dbs are mutually exclusive")
	}

	if c.ExcludeColls != "" && c.IncludeColls != "" {
		panic("exclude_colls or include_colls are mutually exclusive")
	}

	if c.ExcludeDBs == "" && c.IncludeDBs == "" {
		c.ExcludeDBs = "admin,local,config"
	} else if c.ExcludeDBs != "" {
		c.ExcludeDBs += ",admin,local,config"
	}

	if c.IncludeDBs == "" && c.ExcludeColls == "" {
		c.ExcludeColls = "system.profile"
	} else if c.ExcludeColls != "" {
		c.ExcludeColls += ",system.profile"
	}

	if c.LogPath == "" {
		c.LogPath = "./logs"
	}

	if c.LimitQPS == 0 {
		c.LimitQPS = 5000
	}

	if !slices.Contains(vars.ConnModes, c.ConnectMode) {
		modes := strings.Join(vars.ConnModes, ",")
		panic(fmt.Sprintf("conn_modes %s is not one of [%s]", modes, c.ConnectMode))
	}

	if c.ConnectMode == "" {
		c.ConnectMode = "primary"
	}

	if c.Parallel == 0 {
		c.Parallel = 8
	}
}
