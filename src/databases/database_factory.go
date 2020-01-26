// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package databases

import (
	"strings"
	"sync"

	"config"
	"storages"

	"base/errors"
	"base/xlog"
	"parsers/sqlparser"
)

var (
	databases *Databases
	factory   = newDatabaseFactory()
)

type (
	databaseCreator func(*DatabaseContext, *sqlparser.DBDDL) IDatabase
	databaseFactory struct {
		mu       sync.RWMutex
		creators map[string]databaseCreator
	}
)

func newDatabaseFactory() *databaseFactory {
	return &databaseFactory{
		creators: make(map[string]databaseCreator),
	}
}

func (factory *databaseFactory) register(name string, creator databaseCreator) {
	factory.mu.Lock()
	defer factory.mu.Unlock()

	if _, ok := factory.creators[strings.ToUpper(name)]; ok {
		panic(errors.Errorf("database creator name:%s is not unique", name))
	}
	factory.creators[name] = creator
}

func (factory *databaseFactory) get(name string) (databaseCreator, error) {
	factory.mu.RLock()
	defer factory.mu.RUnlock()

	name = strings.ToUpper(name)
	if creator, ok := factory.creators[name]; !ok {
		return nil, errors.Errorf("couldn't get the database creator:%s", name)
	} else {
		return creator, nil
	}
}

func Load(log *xlog.Log, conf *config.Config) error {
	// Engines.
	{
		// System.
		registerSystemDatabase(factory)

		// Ordinary.
		registerOrdinaryDatabase(factory)
	}

	databases = NewDatabases(log, conf)
	return databases.load()
}

func DatabaseFactory(ctx *DatabaseContext, node *sqlparser.DBDDL) (IDatabase, error) {
	name := OrdinaryDatabaseName

	if node.Options != nil {
		name = node.Options.Engine
	}

	creator, err := factory.get(name)
	if err != nil {
		return nil, err
	}
	return creator(ctx, node), nil
}

func GetDatabase(dbname string) (IDatabase, error) {
	if databases == nil {
		return nil, errors.New("Databases need init first.")
	}
	return databases.getDatabase(dbname)
}

func GetStorage(ctx *DatabaseContext, dbname string, tablename string) (storages.IStorage, error) {
	log := ctx.log

	log.Debug("Database->Get->Schema:%v, table:%v", dbname, tablename)
	database, err := GetDatabase(dbname)
	if err != nil {
		return nil, err
	}
	return database.GetStorage(tablename)
}
