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

type SystemTablesStorage struct {
	ctx *SystemStorageContext
}

func NewSystemTablesStorage(ctx *SystemStorageContext) *SystemTablesStorage {
	return &SystemTablesStorage{
		ctx: ctx,
	}
}

func (storage *SystemTablesStorage) Name() string {
	return ""
}

func (storage *SystemTablesStorage) Columns() []*columns.Column {
	return []*columns.Column{
		{Name: "name", DataType: datatypes.NewStringDataType()},
		{Name: "database", DataType: datatypes.NewStringDataType()},
		{Name: "engine", DataType: datatypes.NewStringDataType()},
	}
}

func (storage *SystemTablesStorage) GetOutputStream(session *sessions.Session, scan *planners.ScanPlan) (datablocks.IDataBlockOutputStream, error) {
	return nil, errors.New("Couldn't find outputstream")
}

func (storage *SystemTablesStorage) GetInputStream(session *sessions.Session, scan *planners.ScanPlan) (datablocks.IDataBlockInputStream, error) {
	ctx := storage.ctx

	// Block.
	block := datablocks.NewDataBlock(storage.Columns())
	if err := ctx.tablesFillFunc(block); err != nil {
		return nil, err
	}

	// Stream.
	return datastreams.NewOneBlockInputStream(block), nil
}
