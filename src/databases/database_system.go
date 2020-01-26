// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package databases

import (
	"sync"

	"storages"

	"path/filepath"

	"base/errors"
	"parsers/sqlparser"
)

const (
	SystemDatabaseName = "SYSTEM"
)

type SystemDatabase struct {
	mu          sync.RWMutex
	ctx         *DatabaseContext
	node        *sqlparser.DBDDL
	metaFuns    *MetaFuns
	executeFuns *ExecuteFuns
	tableCaches map[string]*Table
}

func NewSystemDatabase(ctx *DatabaseContext, node *sqlparser.DBDDL) IDatabase {
	database := &SystemDatabase{
		ctx:         ctx,
		node:        node,
		tableCaches: make(map[string]*Table),
	}

	database.executeFuns = &ExecuteFuns{
		CreateDatabase: database.createDatabase,
		DropDatabase:   database.dropDatabase,
		CreateTable:    database.createTable,
		DropTable:      database.dropTable,
	}
	database.metaFuns = &MetaFuns{
		GetDBName:       database.getDBName,
		GetDataPath:     database.getDataPath,
		GetEngineName:   database.getEngineName,
		GetMetaDataPath: database.getMetaDataPath,
	}
	return database
}

func (database *SystemDatabase) Load() error {
	if err := database.attachTable("tables", storages.SystemTablesStorageEngineName); err != nil {
		return err
	}
	if err := database.attachTable("databases", storages.SystemDatabasesStorageEngineName); err != nil {
		return err
	}
	return nil
}

func (database *SystemDatabase) Name() string {
	return OrdinaryDatabaseName
}

func (database *SystemDatabase) Executor() *ExecuteFuns {
	return database.executeFuns
}

func (database *SystemDatabase) Meta() *MetaFuns {
	return database.metaFuns
}

func (database *SystemDatabase) GetTables() []*Table {
	var tables []*Table
	for _, v := range database.tableCaches {
		tables = append(tables, v)
	}
	return tables
}

func (database *SystemDatabase) GetStorage(tableName string) (storages.IStorage, error) {
	database.mu.RLock()
	defer database.mu.RUnlock()

	table, ok := database.tableCaches[tableName]
	if !ok {
		return nil, errors.Errorf("Couldn't find table:%v storage", tableName)
	}
	return table.storage, nil
}

func (database *SystemDatabase) attachTable(tableName string, engine string) error {
	database.mu.Lock()
	defer database.mu.Unlock()

	ctx := database.ctx
	log := ctx.log
	conf := ctx.conf
	dbName := database.getDBName()

	if _, ok := database.tableCaches[tableName]; ok {
		return errors.Errorf("%s.%s exists", dbName, tableName)
	}

	storageCtx := storages.NewStorageContext(ctx.log, ctx.conf)
	storageCtx.SetTablesFillFunc(fillTablesFunc)
	storageCtx.SetDatabasesFillFunc(fillDatabasesFunc)
	storage, err := storages.StorageFactory(storageCtx, engine, nil)
	if err != nil {
		return err
	}
	table := NewTable(conf, dbName, tableName, engine, nil, storage)
	database.tableCaches[tableName] = table
	log.Debug("Database->Attach Table:%s.%s, engine:%s", dbName, tableName, engine)
	return nil
}

// Execute handlers.
func (database *SystemDatabase) createDatabase() error {
	return errors.New("Couldn't create the system database")
}

func (database *SystemDatabase) dropDatabase() error {
	return databases.detachDatabase(SystemDatabaseName)
}

func (database *SystemDatabase) createTable(node *sqlparser.DDL) error {
	return errors.New("Couldn't create the system table")
}

func (database *SystemDatabase) dropTable(tableName string) error {
	return errors.New("Couldn't drop the system table")
}

// Meta handlers.
func (database *SystemDatabase) getDBName() string {
	return database.node.DBName
}

func (database *SystemDatabase) getDataPath() string {
	node := database.node
	conf := database.ctx.conf

	return filepath.Join(conf.Server.Path, "data", node.DBName)
}

func (database *SystemDatabase) getEngineName() string {
	return database.node.Options.Engine
}

func (database *SystemDatabase) getMetaDataPath() string {
	node := database.node
	conf := database.ctx.conf

	return filepath.Join(conf.Server.Path, "metadata", node.DBName)
}

func registerSystemDatabase(factory *databaseFactory) {
	factory.register(SystemDatabaseName, NewSystemDatabase)
}
