// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package binary

import (
	"io"
	"math"

	"encoding/binary"
)

type Reader struct {
	input io.Reader
	datas [binary.MaxVarintLen64]byte
}

func NewReader(input io.Reader) *Reader {
	return &Reader{
		input: input,
	}
}

func (reader *Reader) Bool() (bool, error) {
	v, err := reader.ReadByte()
	if err != nil {
		return false, err
	}
	return v == 1, nil
}

func (reader *Reader) Uvarint() (uint64, error) {
	return binary.ReadUvarint(reader)
}

func (reader *Reader) Int8() (int8, error) {
	v, err := reader.ReadByte()
	if err != nil {
		return 0, err
	}
	return int8(v), nil
}

func (reader *Reader) UInt8() (uint8, error) {
	v, err := reader.ReadByte()
	if err != nil {
		return 0, err
	}
	return uint8(v), nil
}

func (reader *Reader) Int32() (int32, error) {
	v, err := reader.UInt32()
	if err != nil {
		return 0, err
	}
	return int32(v), nil
}

func (reader *Reader) UInt32() (uint32, error) {
	if _, err := reader.input.Read(reader.datas[:4]); err != nil {
		return 0, err
	}
	return uint32(reader.datas[0]) |
		uint32(reader.datas[1])<<8 |
		uint32(reader.datas[2])<<16 |
		uint32(reader.datas[3])<<24, nil
}

func (reader *Reader) Int64() (int64, error) {
	v, err := reader.UInt64()
	if err != nil {
		return 0, err
	}
	return int64(v), nil
}

func (reader *Reader) UInt64() (uint64, error) {
	if _, err := reader.input.Read(reader.datas[:8]); err != nil {
		return 0, err
	}
	return uint64(reader.datas[0]) |
		uint64(reader.datas[1])<<8 |
		uint64(reader.datas[2])<<16 |
		uint64(reader.datas[3])<<24 |
		uint64(reader.datas[4])<<32 |
		uint64(reader.datas[5])<<40 |
		uint64(reader.datas[6])<<48 |
		uint64(reader.datas[7])<<56, nil
}

func (reader *Reader) Float32() (float32, error) {
	v, err := reader.UInt32()
	if err != nil {
		return 0, err
	}
	return math.Float32frombits(v), nil
}

func (reader *Reader) Float64() (float64, error) {
	v, err := reader.UInt64()
	if err != nil {
		return 0, err
	}
	return math.Float64frombits(v), nil
}

func (reader *Reader) Bytes(ln int) ([]byte, error) {
	buf := make([]byte, ln)
	if _, err := reader.input.Read(buf); err != nil {
		return nil, err
	}
	return buf, nil
}

func (reader *Reader) String() (string, error) {
	len, err := reader.Uvarint()
	if err != nil {
		return "", err
	}
	str, err := reader.Bytes(int(len))
	if err != nil {
		return "", err
	}
	return string(str), nil
}

func (reader *Reader) ReadByte() (byte, error) {
	if _, err := reader.input.Read(reader.datas[:1]); err != nil {
		return 0x0, err
	}
	return reader.datas[0], nil
}
