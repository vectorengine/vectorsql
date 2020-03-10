// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package datastreams

import (
	"base/binary"
	"base/errors"
	"columns"
	"datablocks"
	"datatypes"
	"datavalues"
)

type NativeBlockInputStream struct {
	reader *binary.Reader
}

func NewNativeBlockInputStream(reader *binary.Reader) IDataBlockInputStream {
	return &NativeBlockInputStream{
		reader: reader,
	}
}

func (stream *NativeBlockInputStream) Name() string {
	return "NativeBlockInputStream"
}

func (stream *NativeBlockInputStream) Read() (*datablocks.DataBlock, error) {
	var err error
	var numRows uint64
	var numColumns uint64

	reader := stream.reader

	// Temporary table.
	if _, err = reader.String(); err != nil {
		return nil, errors.Wrap(err)
	}

	info := datablocks.DataBlockInfo{}
	if err := info.Read(reader); err != nil {
		return nil, err
	}

	// NumColumns.
	if numColumns, err = reader.Uvarint(); err != nil {
		return nil, errors.Wrap(err)
	}
	// NumRows.
	if numRows, err = reader.Uvarint(); err != nil {
		return nil, errors.Wrap(err)
	}

	columnSlice := make([]*columns.Column, numColumns)
	valueSlice := make([][]datavalues.IDataValue, numColumns)
	for i := 0; i < int(numColumns); i++ {
		colName, err := reader.String()
		if err != nil {
			return nil, err
		}
		typeName, err := reader.String()
		if err != nil {
			return nil, err
		}
		dt, err := datatypes.DataTypeFactory(typeName)
		if err != nil {
			return nil, err
		}
		columnSlice[i] = columns.NewColumn(colName, dt)
		values := make([]datavalues.IDataValue, numRows)
		for j := 0; j < int(numRows); j++ {
			val, err := dt.Deserialize(reader)
			if err != nil {
				return nil, err
			}
			values[j] = val
		}
		valueSlice[i] = values
	}

	if numColumns > 0 {
		block := datablocks.NewDataBlock(columnSlice)
		for i := 0; i < int(numRows); i++ {
			row := make([]datavalues.IDataValue, numColumns)
			for j := 0; j < int(numColumns); j++ {
				row[j] = valueSlice[j][i]
			}
			if err := block.WriteRow(row); err != nil {
				return nil, err
			}
		}
		return block, nil
	}
	return nil, nil
}

func (stream *NativeBlockInputStream) Close() {}
