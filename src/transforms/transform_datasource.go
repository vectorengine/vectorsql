// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package transforms

import (
	"datastreams"
	"processors"
	"sync/atomic"
)

type DataSourceTransform struct {
	ctx         *TransformContext
	input       datastreams.IDataBlockInputStream
	processRows int64
	processors.BaseProcessor
}

func NewDataSourceTransform(ctx *TransformContext, input datastreams.IDataBlockInputStream) processors.IProcessor {
	return &DataSourceTransform{
		ctx:           ctx,
		input:         input,
		BaseProcessor: processors.NewBaseProcessor("transform_datasource"),
	}
}

func (t *DataSourceTransform) Execute() {
	ctx := t.ctx
	log := ctx.log
	input := t.input
	out := t.Out()

	defer out.Close()
	for {
		select {
		case <-ctx.ctx.Done():
			return
		default:
			if out.IsClose() {
				return
			}
			data, err := input.Read()
			if err != nil {
				log.Error("Transform->Input error:%+v", err)
				out.Send(err)
				return
			} else if data == nil {
				return
			}
			out.Send(data)
			atomic.AddInt64(&t.processRows, int64(data.NumRows()))
		}

	}
}

func (t *DataSourceTransform) Rows() int64 {
	return atomic.LoadInt64(&t.processRows)
}
