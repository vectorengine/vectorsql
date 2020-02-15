// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package system

import (
	"columns"
	"datablocks"
	"datastreams"
	"datatypes"
	"planners"
	"sessions"

	"base/errors"
)

type SystemDatabasesStorage struct {
	ctx *SystemStorageContext
}

func NewSystemDatabasesStorage(ctx *SystemStorageContext) *SystemDatabasesStorage {
	return &SystemDatabasesStorage{
		ctx: ctx,
	}
}

func (storage *SystemDatabasesStorage) Name() string {
	return ""
}

func (storage *SystemDatabasesStorage) Columns() []*columns.Column {
	return []*columns.Column{
		{Name: "name", DataType: datatypes.NewStringDataType()},
		{Name: "engine", DataType: datatypes.NewStringDataType()},
		{Name: "data_path", DataType: datatypes.NewStringDataType()},
		{Name: "metadata_path", DataType: datatypes.NewStringDataType()},
	}
}

func (storage *SystemDatabasesStorage) GetOutputStream(session *sessions.Session, scan *planners.ScanPlan) (datastreams.IDataBlockOutputStream, error) {
	return nil, errors.New("Couldn't find outputstream")
}

func (storage *SystemDatabasesStorage) GetInputStream(session *sessions.Session, scan *planners.ScanPlan) (datastreams.IDataBlockInputStream, error) {
	ctx := storage.ctx
	log := ctx.log

	// Block.
	block := datablocks.NewDataBlock(storage.Columns())
	if err := ctx.databasesFillFunc(block); err != nil {
		return nil, err
	}
	log.Debug("Storage->System->Block:%+v", block)

	// Stream.
	return datastreams.NewOneBlockInputStream(block), nil
}
