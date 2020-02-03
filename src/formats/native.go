// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package formats

import (
	"datablocks"
)

type NativeOutputFormat struct {
	datablocks.IDataBlockOutputStream
	DataBlockOutputFormatBase
}
