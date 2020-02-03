// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package formats

type DataBlockOutputFormatBase struct {
}

func (f *DataBlockOutputFormatBase) FormatPrefix() (b []byte, err error) {
	return
}

func (f *DataBlockOutputFormatBase) FormatSuffix() (b []byte, err error) {
	return
}
