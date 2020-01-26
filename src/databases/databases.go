// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package databases

import (
	"sync"

	"config"
	"parsers"

	"io/ioutil"
	"path/filepath"

	"base/errors"
	"base/xlog"
	"parsers/sqlparser"
)

type Databases struct {
	mu        sync.RWMutex
	log       *xlog.Log
	conf      *config.Config
	databases map[string]IDatabase
}

func NewDatabases(log *xlog.Log, conf *config.Config) *Databases {
	return &Databases{
		log:       log,
		conf:      conf,
		databases: make(map[string]IDatabase),
	}
}

func (db *Databases) load() error {
	if err := db.loadSystemDatabases(); err != nil {
		return err
	}
	if err := db.loadDiskDatabases(); err != nil {
		return err
	}
	return nil
}

func (db *Databases) loadDiskDatabases() error {
	log := db.log
	conf := db.conf

	databaseMetaFiles := filepath.Join(conf.Server.Path, "metadata", "*.sql")
	files, _ := filepath.Glob(databaseMetaFiles)
	for _, file := range files {
		query, err := ioutil.ReadFile(file)
		if err != nil {
			return errors.Wrap(err)
		}

		node, err := parsers.Parse(string(query))
		if err != nil {
			return err
		}
		dbddl := node.(*sqlparser.DBDDL)
		// Set default database engine.
		if dbddl.Options == nil {
			dbddl.Options = &sqlparser.DatabaseOption{Engine: OrdinaryDatabaseName}
		}

		ctx := NewDatabaseContext(log, conf)
		database, err := DatabaseFactory(ctx, dbddl)
		if err != nil {
			return err
		}
		if err := database.Load(); err != nil {
			return err
		}
		if err := db.attachDatabase(dbddl.DBName, database); err != nil {
			return err
		}
		log.Info("Load database:%s, file:%s", dbddl.DBName, file)
	}
	return nil
}

func (db *Databases) loadSystemDatabases() error {
	log := db.log
	conf := db.conf

	querys := []string{
		"CREATE DATABASE system ENGINE=SYSTEM",
	}

	for _, query := range querys {
		node, err := parsers.Parse(string(query))
		if err != nil {
			return err
		}
		dbddl := node.(*sqlparser.DBDDL)

		ctx := NewDatabaseContext(log, conf)
		database, err := DatabaseFactory(ctx, dbddl)
		if err != nil {
			return err
		}
		if err := database.Load(); err != nil {
			return err
		}
		if err := db.attachDatabase(dbddl.DBName, database); err != nil {
			return err
		}
		log.Info("Database->Load Database:%s", dbddl.DBName)
	}
	return nil
}

func (db *Databases) attachDatabase(dbname string, database IDatabase) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if _, ok := db.databases[dbname]; ok {
		return errors.Errorf("database:%s exists", dbname)
	}
	db.databases[dbname] = database
	return nil
}

func (db *Databases) detachDatabase(dbname string) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if _, ok := db.databases[dbname]; !ok {
		return errors.Errorf("database:%s doesn't exists", dbname)
	}
	delete(db.databases, dbname)
	return nil
}

func (db *Databases) getDatabase(dbname string) (IDatabase, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if _, ok := db.databases[dbname]; !ok {
		return nil, errors.Errorf("database:%s doesn't exists", dbname)
	}
	return db.databases[dbname], nil
}
