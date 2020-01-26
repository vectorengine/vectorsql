// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package config

import (
	"os"

	"github.com/naoina/toml"
)

type Server struct {
	TCPPort                 int
	HTTPPort                int
	Path                    string
	TmpPath                 string
	ListenHost              string
	DisplayName             string
	DefaultDatabase         string
	CalculateTextStackTrace bool
}

func DefaultServerConfig() Server {
	return Server{
		TCPPort:         9000,
		HTTPPort:        8123,
		Path:            "./data9000",
		DefaultDatabase: "default",
		DisplayName:     "VectorSQL",
	}
}

type Logger struct {
	Level string
}

func DefaultLoggerConfig() Logger {
	return Logger{
		Level: "DEBUG",
	}
}

func DefaultConfig() *Config {
	return &Config{
		Server: DefaultServerConfig(),
		Logger: DefaultLoggerConfig(),
	}
}

type Config struct {
	Server Server
	Logger Logger
}

func Load(file string) (*Config, error) {
	f, err := os.Open(file)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	conf := DefaultConfig()
	if err := toml.NewDecoder(f).Decode(conf); err != nil {
		return nil, err
	}
	return conf, nil
}
