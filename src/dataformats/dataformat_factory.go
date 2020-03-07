// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package dataformats

import (
	"io"
)

type (
	InputCreator  func(reader io.Reader) IDataBlockInputFormat
	OutputCreator func(writer io.Writer) IDataBlockOutputFormat
)

var (
	inputTable  = map[string]InputCreator{}
	outputTable = map[string]OutputCreator{
		"TSV":                   NewTSVOutputFormat,
		"TabSeparated":          NewTSVOutputFormat,
		"TSVWithNames":          NewTSVWithNamesOutputFormat,
		"TabSeparatedWithNames": NewTSVWithNamesOutputFormat,
	}
)

func FactoryGetInput(name string) InputCreator {
	return inputTable[name]
}

func FactoryGetOutput(name string) OutputCreator {
	return outputTable[name]
}
