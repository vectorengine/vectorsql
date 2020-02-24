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
	DebugPort               int
	Path                    string
	TmpPath                 string
	ListenHost              string
	DisplayName             string
	DefaultDatabase         string
	DefaultBlockSize        int
	CalculateTextStackTrace bool
}

func DefaultServerConfig() Server {
	return Server{
		TCPPort:          9000,
		HTTPPort:         8123,
		DebugPort:        8080,
		Path:             "./data9000",
		DisplayName:      "VectorSQL",
		DefaultDatabase:  "default",
		DefaultBlockSize: 65536,
	}
}

type Runtime struct {
	ParallelWorkerNumber int
}

func DefaultRuntimeConfig() Runtime {
	return Runtime{
		ParallelWorkerNumber: 4,
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
		Server:  DefaultServerConfig(),
		Runtime: DefaultRuntimeConfig(),
		Logger:  DefaultLoggerConfig(),
	}
}

type Config struct {
	Server  Server
	Runtime Runtime
	Logger  Logger
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
