// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package databases

import (
	"os"
	"sync"

	"io/ioutil"
	"path/filepath"

	"columns"
	"datatypes"
	"parsers"
	"storages"

	"base/errors"
	"parsers/sqlparser"
)

type OnDiskDatabase struct {
	mu          sync.RWMutex
	ctx         *DatabaseContext
	node        *sqlparser.DBDDL
	metaFuns    *MetaFuns
	executeFuns *ExecuteFuns
	tableCaches map[string]*Table
}

func NewOnDiskDatabase(ctx *DatabaseContext, node *sqlparser.DBDDL) IDatabase {
	database := &OnDiskDatabase{
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

func (database *OnDiskDatabase) Load() error {
	ctx := database.ctx
	log := ctx.log
	conf := ctx.conf
	dbName := database.node.DBName
	tablefiles := filepath.Join(conf.Server.Path, "metadata", dbName, "*.sql")

	files, _ := filepath.Glob(tablefiles)
	for _, file := range files {
		query, err := ioutil.ReadFile(file)
		if err != nil {
			return errors.Wrap(err)
		}

		node, err := parsers.Parse(string(query))
		if err != nil {
			return err
		}
		ddl := node.(*sqlparser.DDL)
		tableName := ddl.Table.Name.String()
		if err := database.attachTable(tableName, ddl); err != nil {
			return err
		}
		log.Info("Load table:%v, file:%s", ddl.Table, file)
	}
	return nil
}

func (database *OnDiskDatabase) Name() string {
	return ""
}

func (database *OnDiskDatabase) Executor() *ExecuteFuns {
	return database.executeFuns
}

func (database *OnDiskDatabase) Meta() *MetaFuns {
	return database.metaFuns
}

func (database *OnDiskDatabase) GetTables() []*Table {
	var tables []*Table
	for _, v := range database.tableCaches {
		tables = append(tables, v)
	}
	return tables
}

func (database *OnDiskDatabase) GetStorage(tableName string) (storages.IStorage, error) {
	database.mu.RLock()
	defer database.mu.RUnlock()

	table, ok := database.tableCaches[tableName]
	if !ok {
		return nil, errors.Errorf("couldn't find table:%v storage", tableName)
	}
	return table.storage, nil
}

func (database *OnDiskDatabase) attachTable(tableName string, node *sqlparser.DDL) error {
	ctx := database.ctx
	log := ctx.log
	conf := ctx.conf
	dbName := database.node.DBName
	engine := node.TableSpec.Options.Engine

	database.mu.Lock()
	defer database.mu.Unlock()
	if _, ok := database.tableCaches[tableName]; ok {
		return errors.Errorf("%s.%s exists", dbName, tableName)
	}

	var colDefinitions []*sqlparser.ColumnDefinition
	if err := sqlparser.Walk(func(node sqlparser.SQLNode) (bool, error) {
		switch node := node.(type) {
		case *sqlparser.ColumnDefinition:
			colDefinitions = append(colDefinitions, node)
		}
		return true, nil
	}, node.TableSpec); err != nil {
		return err
	}

	cols := make([]*columns.Column, len(colDefinitions))
	for i, coldef := range colDefinitions {
		dataType, err := datatypes.DataTypeFactory(coldef.Type.Type)
		if err != nil {
			return err
		}
		cols[i] = columns.NewColumn(coldef.Name.String(), dataType)
	}

	storageCtx := storages.NewStorageContext(ctx.log, ctx.conf)
	storage, err := storages.StorageFactory(storageCtx, engine, cols)
	if err != nil {
		return err
	}
	table := NewTable(conf, dbName, tableName, engine, node, storage)
	database.tableCaches[tableName] = table
	log.Info("Attach table:%s.%s, engine:%s", dbName, tableName, engine)
	return nil
}

func (database *OnDiskDatabase) detachTable(tableName string) error {
	database.mu.Lock()
	defer database.mu.Unlock()

	log := database.ctx.log
	dbName := database.getDBName()
	tbl, ok := database.tableCaches[tableName]
	if !ok {
		return errors.Errorf("%s.%s doesn't exists", dbName, tableName)
	}
	tbl.storage.Close()
	delete(database.tableCaches, tableName)
	log.Info("Detach table:%s.%s", dbName, tableName)
	return nil
}

// Execute  handlers.
func (database *OnDiskDatabase) createDatabase() error {
	log := database.ctx.log
	dbName := database.getDBName()
	dataPath := database.getDataPath()
	metaDataPath := database.getMetaDataPath()
	metaFile := metaDataPath + ".sql"

	// Check.
	if _, err := GetDatabase(dbName); err == nil {
		return errors.Errorf("database:%v exists", dbName)
	}

	if err := os.MkdirAll(dataPath, os.ModePerm); err != nil {
		return errors.Wrap(err)
	}
	if err := os.MkdirAll(metaDataPath, os.ModePerm); err != nil {
		return errors.Wrap(err)
	}

	buf := sqlparser.NewTrackedBuffer(nil)
	database.node.Format(buf)
	query := buf.String()
	log.Debug("%s: %s", metaFile, query)
	if err := ioutil.WriteFile(metaFile, []byte(query), 0644); err != nil {
		return errors.Wrap(err)
	}

	log.Info("Create database:%s, meta:%s", dbName, metaDataPath)
	return databases.attachDatabase(dbName, database)
}

func (database *OnDiskDatabase) dropDatabase() error {
	log := database.ctx.log
	dbName := database.getDBName()
	dataPath := database.getDataPath()
	metaDataPath := database.getMetaDataPath()
	metaFile := metaDataPath + ".sql"

	for tableName := range database.tableCaches {
		if err := database.dropTable(tableName); err != nil {
			return err
		}
	}

	if err := os.RemoveAll(dataPath); err != nil {
		return errors.Wrap(err)
	}
	if err := os.RemoveAll(metaDataPath); err != nil {
		return errors.Wrap(err)
	}
	if err := os.RemoveAll(metaFile); err != nil {
		return errors.Wrap(err)
	}
	log.Info("Drop database:%s, meta:%s", dbName, metaFile)
	return databases.detachDatabase(dbName)
}

func (database *OnDiskDatabase) createTable(node *sqlparser.DDL) error {
	log := database.ctx.log
	tableName := node.Table.Name.String()
	tableDataPath := filepath.Join(database.getDataPath(), tableName)
	tableMetaFile := filepath.Join(database.getMetaDataPath(), tableName) + ".sql"

	buf := sqlparser.NewTrackedBuffer(nil)
	node.Format(buf)
	query := buf.String()

	if err := ioutil.WriteFile(tableMetaFile, []byte(query), 0644); err != nil {
		return errors.Wrap(err)
	}
	if err := os.MkdirAll(tableDataPath, os.ModePerm); err != nil {
		return errors.Wrap(err)
	}
	log.Info("Create table:%s, query:%s", tableMetaFile, query)
	return database.attachTable(tableName, node)
}

func (database *OnDiskDatabase) dropTable(tableName string) error {
	log := database.ctx.log
	dbName := database.getDBName()
	tableDataPath := filepath.Join(database.getDataPath(), tableName)
	tableMetaFile := filepath.Join(database.getMetaDataPath(), tableName) + ".sql"

	if err := os.RemoveAll(tableMetaFile); err != nil {
		return errors.Wrap(err)
	}
	if err := os.RemoveAll(tableDataPath); err != nil {
		return errors.Wrap(err)
	}
	log.Info("Drop table:%s.%s", dbName, tableName)
	return database.detachTable(tableName)
}

// Meta handlers.
func (database *OnDiskDatabase) getDBName() string {
	return database.node.DBName
}

func (database *OnDiskDatabase) getDataPath() string {
	node := database.node
	conf := database.ctx.conf

	return filepath.Join(conf.Server.Path, "data", node.DBName)
}

func (database *OnDiskDatabase) getEngineName() string {
	if database.node.Options != nil {
		return database.node.Options.Engine
	}
	return ""
}

func (database *OnDiskDatabase) getMetaDataPath() string {
	node := database.node
	conf := database.ctx.conf

	return filepath.Join(conf.Server.Path, "metadata", node.DBName)
}
