// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package storages

import (
	"sync"

	"columns"
	"datablocks"
	"datastreams"
	"planners"
	"sessions"
)

type MemoryStorage struct {
	ctx    *MemoryStorageContext
	cols   []columns.Column
	output *NativeBlockOutputStream
}

func NewMemoryStorage(ctx *MemoryStorageContext, cols []columns.Column) *MemoryStorage {
	return &MemoryStorage{
		ctx:    ctx,
		cols:   cols,
		output: NewNativeBlockOutputStream(),
	}
}

func (storage *MemoryStorage) Name() string {
	return "Memory"
}

func (storage *MemoryStorage) Columns() []columns.Column {
	return storage.cols
}

func (storage *MemoryStorage) GetOutputStream(session *sessions.Session, scan *planners.ScanPlan) (datablocks.IDataBlockOutputStream, error) {
	return storage.output, nil
}

func (storage *MemoryStorage) GetInputStream(session *sessions.Session, scan *planners.ScanPlan) (datablocks.IDataBlockInputStream, error) {
	log := storage.ctx.log

	log.Debug("Storage->Memory->Enter->Database:%v, Project:%+v, Filter:%+v",
		session.GetDatabase(),
		scan.Project != nil,
		scan.Filter != nil,
	)

	// Stream.
	stream := datastreams.NewOneBlockInputStream(storage.output.blocks[0])
	log.Debug("Storage->Memory->Return->Stream:%+v", stream)
	return stream, nil
}

type NativeBlockOutputStream struct {
	mu     sync.RWMutex
	blocks []*datablocks.DataBlock
}

func NewNativeBlockOutputStream() *NativeBlockOutputStream {
	return &NativeBlockOutputStream{}
}

func (stream *NativeBlockOutputStream) Name() string {
	return "NativeBlockOutputStream"
}

func (stream *NativeBlockOutputStream) Write(block *datablocks.DataBlock) error {
	stream.mu.Lock()
	defer stream.mu.Unlock()
	stream.blocks = append(stream.blocks, block)
	return nil
}
func (stream *NativeBlockOutputStream) Finalize() error {
	return nil
}
