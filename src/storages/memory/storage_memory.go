// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package storages

import (
	"sync"

	"columns"
	"datablocks"
	"datastreams"
	"sessions"
)

type MemoryStorage struct {
	ctx    *MemoryStorageContext
	cols   []*columns.Column
	output *NativeBlockOutputStream
}

func NewMemoryStorage(ctx *MemoryStorageContext, cols []*columns.Column) *MemoryStorage {
	return &MemoryStorage{
		ctx:    ctx,
		cols:   cols,
		output: NewNativeBlockOutputStream(datablocks.NewDataBlock(cols)),
	}
}

func (storage *MemoryStorage) Name() string {
	return "Memory"
}

func (storage *MemoryStorage) Columns() []*columns.Column {
	return storage.cols
}

func (storage *MemoryStorage) GetOutputStream(session *sessions.Session) (datastreams.IDataBlockOutputStream, error) {
	return storage.output, nil
}

func (storage *MemoryStorage) GetInputStream(session *sessions.Session) (datastreams.IDataBlockInputStream, error) {
	log := storage.ctx.log

	i := 0
	iteratorFn := func() (*datablocks.DataBlock, error) {
		if i >= len(storage.output.blocks) {
			return nil, nil
		}
		res := storage.output.blocks[i].DeepClone()
		log.Debug("Storage->Memory->InputStream->Block: index:%v, rows:%v", i, res.NumRows())
		i++
		return res, nil
	}
	stream := datastreams.NewIteratorBlockInputStream(iteratorFn)
	return stream, nil
}

func (storage *MemoryStorage) Close() {
	storage.cols = nil
	storage.output.Close()
}

type NativeBlockOutputStream struct {
	mu     sync.RWMutex
	header *datablocks.DataBlock
	blocks []*datablocks.DataBlock
}

func NewNativeBlockOutputStream(header *datablocks.DataBlock) *NativeBlockOutputStream {
	return &NativeBlockOutputStream{header: header}
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

func (stream *NativeBlockOutputStream) Close() {
	for _, block := range stream.blocks {
		block.Close()
	}
	stream.blocks = nil
	stream.header = nil
}

func (stream *NativeBlockOutputStream) SampleBlock() *datablocks.DataBlock {
	return stream.header.Clone()
}
