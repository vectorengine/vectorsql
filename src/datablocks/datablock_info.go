// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package datablocks

import (
	"base/binary"
	"base/errors"
)

type DataBlockInfo struct {
	Num1        uint64
	IsOverflows bool
	Num2        uint64
	BucketNum   int32
	Num3        uint64
}

func (info *DataBlockInfo) Read(reader *binary.Reader) error {
	var err error
	if info.Num1, err = reader.Uvarint(); err != nil {
		return errors.Wrap(err)
	}
	if info.IsOverflows, err = reader.Bool(); err != nil {
		return errors.Wrap(err)
	}
	if info.Num2, err = reader.Uvarint(); err != nil {
		return errors.Wrap(err)
	}
	if info.BucketNum, err = reader.Int32(); err != nil {
		return errors.Wrap(err)
	}
	if info.Num3, err = reader.Uvarint(); err != nil {
		return errors.Wrap(err)
	}
	return nil
}

func (info *DataBlockInfo) Write(writer *binary.Writer) error {
	if err := writer.Uvarint(1); err != nil {
		return errors.Wrap(err)
	}
	if err := writer.Bool(info.IsOverflows); err != nil {
		return errors.Wrap(err)
	}
	if err := writer.Uvarint(2); err != nil {
		return errors.Wrap(err)
	}
	if info.BucketNum == 0 {
		info.BucketNum = -1
	}
	if err := writer.Int32(info.BucketNum); err != nil {
		return errors.Wrap(err)
	}
	if err := writer.Uvarint(0); err != nil {
		return errors.Wrap(err)
	}
	return nil
}
