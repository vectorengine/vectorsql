// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package binary

import (
	"bufio"
	"io"

	"encoding/binary"
)

type Writer struct {
	output io.Writer
	datas  [binary.MaxVarintLen64]byte
}

func NewWriter(w io.Writer) *Writer {
	return &Writer{
		output: w,
	}
}

func (writer *Writer) Bool(v bool) error {
	if v {
		return writer.UInt8(1)
	}
	return writer.UInt8(0)
}

func (writer *Writer) UInt8(v uint8) error {
	writer.datas[0] = v
	if _, err := writer.output.Write(writer.datas[:1]); err != nil {
		return err
	}
	return nil
}

func (writer *Writer) UInt32(v uint32) error {
	writer.datas[0] = byte(v)
	writer.datas[1] = byte(v >> 8)
	writer.datas[2] = byte(v >> 16)
	writer.datas[3] = byte(v >> 24)
	if _, err := writer.output.Write(writer.datas[:4]); err != nil {
		return err
	}
	return nil
}

func (writer *Writer) Int32(v int32) error {
	return writer.UInt32(uint32(v))
}

func (writer *Writer) Uvarint(v uint64) error {
	ln := binary.PutUvarint(writer.datas[:binary.MaxVarintLen64], v)
	if _, err := writer.output.Write(writer.datas[0:ln]); err != nil {
		return err
	}
	return nil
}

func (writer *Writer) String(v string) error {
	if err := writer.Uvarint(uint64(len(v))); err != nil {
		return err
	}
	if _, err := writer.output.Write([]byte(v)); err != nil {
		return err
	}
	return nil
}

func (writer *Writer) Bytes(str []byte) error {
	if err := writer.Uvarint(uint64(len(str))); err != nil {
		return err
	}
	if _, err := writer.output.Write(str); err != nil {
		return err
	}
	return nil
}

func (writer *Writer) Write(b []byte) (int, error) {
	return writer.output.Write(b)
}

func (writer *Writer) Flush() error {
	w := bufio.NewWriter(writer.output)
	return w.Flush()
}
