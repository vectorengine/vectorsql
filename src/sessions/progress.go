// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package sessions

import (
	"time"
)

type ProgressValues struct {
	Cost            time.Duration
	ReadRows        uint64
	ReadBytes       uint64
	TotalRowsToRead uint64
	WrittenRows     uint64
	WrittenBytes    uint64
}
