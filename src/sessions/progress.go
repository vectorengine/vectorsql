// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package sessions

import (
	"base/sync2"
)

type ProgressValues struct {
	Cost            sync2.AtomicDuration
	ReadRows        sync2.AtomicInt64
	ReadBytes       sync2.AtomicInt64
	TotalRowsToRead sync2.AtomicInt64
	WrittenRows     sync2.AtomicInt64
	WrittenBytes    sync2.AtomicInt64
}
