// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package storages

import (
	"columns"

	mem "storages/memory"
)

const (
	MemoryStorageEngineName = "MEMORY"
)

func NewMemoryStorage(ctx *StorageContext, cols []*columns.Column) IStorage {
	mctx := mem.NewMemoryStorageContext(ctx.log, ctx.conf)
	return mem.NewMemoryStorage(mctx, cols)
}
