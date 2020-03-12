// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"base/humanize"
	"base/xlog"
	"config"
	"databases"
	"servers"
)

var (
	flagConf string
)

func init() {
	flag.StringVar(&flagConf, "c", "", "VectorSQL config file")
	flag.StringVar(&flagConf, "config", "", "VectorSQL config file")
}

func usage() {
	fmt.Println("Usage: " + os.Args[0] + " [-c|--config] <vectorsql-config-file>")
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	log := xlog.NewStdLog(xlog.Level(xlog.DEBUG))

	// Load config.
	flag.Usage = func() { usage() }
	flag.Parse()
	if flagConf == "" {
		usage()
		os.Exit(0)
	}
	conf, err := config.Load(flagConf)
	if err != nil {
		log.Panic("Couldn't load config: %+v", err)
	}
	log.SetLevel(conf.Logger.Level)
	log.Info("Config: %+v", conf)

	// Load database.
	if err := databases.Load(log, conf); err != nil {
		log.Panic("%+v", err)
	}

	// Servers.
	server := servers.NewServer(log, conf)
	server.Start()
	defer server.Stop()
	log.Info("Servers start...")

	go logMemStats(log)

	// Handle signal.
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	<-ch
}

func logMemStats(log *xlog.Log) {
	t := time.NewTicker(10 * time.Second)
	defer t.Stop()

	for range t.C {
		memstats := &runtime.MemStats{}
		runtime.ReadMemStats(memstats)
		log.Info("Memory InUse: %v    Alloc: %v    Sys: %v    NumGC: %v    NextGC: %v",
			humanize.Bytes(memstats.HeapInuse),
			humanize.Bytes(memstats.Alloc),
			humanize.Bytes(memstats.Sys),
			memstats.NumGC,
			humanize.Bytes(memstats.NextGC),
		)
	}
}
