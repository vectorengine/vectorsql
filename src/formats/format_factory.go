// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package formats

import (
	"datablocks"
	"datastreams"
	"io"
)

type (
	InputCreator  func(sampleBlock *datablocks.DataBlock, reader io.Reader) datablocks.IDataBlockInputFormat
	OutputCreator func(sampleBlock *datablocks.DataBlock, writer io.Writer) datablocks.IDataBlockOutputFormat
)

var (
	inputTable  = map[string]InputCreator{}
	outputTable = map[string]OutputCreator{
		"Native": func(_ *datablocks.DataBlock, writer io.Writer) datablocks.IDataBlockOutputFormat {
			return &NativeOutputFormat{
				IDataBlockOutputStream: datastreams.NewNativeBlockOutputStream(writer),
			}
		},

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
