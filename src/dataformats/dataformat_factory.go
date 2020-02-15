// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package dataformats

import (
	"datablocks"
	"io"
)

type (
	InputCreator  func(sampleBlock *datablocks.DataBlock, reader io.Reader) IDataBlockInputFormat
	OutputCreator func(sampleBlock *datablocks.DataBlock, writer io.Writer) IDataBlockOutputFormat
)

var (
	inputTable  = map[string]InputCreator{}
	outputTable = map[string]OutputCreator{
		"TSV":         NewTSVOutputFormat,
		"TabSeparted": NewTSVOutputFormat,

		"TSVWithNames":         NewTSVWithNamesOutputFormat,
		"TabSepartedWithNames": NewTSVWithNamesOutputFormat,
	}
)

func FactoryGetInput(name string) InputCreator {
	return inputTable[name]
}

func FactoryGetOutput(name string) OutputCreator {
	return outputTable[name]
}
