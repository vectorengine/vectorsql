// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package transforms

import (
	"time"

	"datastreams"
	"processors"
	"sessions"
)

type DataSourceTransform struct {
	ctx            *TransformContext
	input          datastreams.IDataBlockInputStream
	progressValues sessions.ProgressValues
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
			start := time.Now()
			data, err := input.Read()
			if err != nil {
				log.Error("Transform->Input error:%+v", err)
				out.Send(err)
				return
			} else if data == nil {
				return
			}
			cost := time.Since(start)
			t.progressValues.Cost.Add(cost)
			t.progressValues.ReadBytes.Add(int64(data.TotalBytes()))
			t.progressValues.ReadRows.Add(int64(data.NumRows()))
			t.progressValues.TotalRowsToRead.Add(int64(data.NumRows()))
			if ctx.progressCallback != nil {
				ctx.progressCallback(&t.progressValues)
			}

			out.Send(data)
		}

	}
}

func (t *DataSourceTransform) Stats() sessions.ProgressValues {
	return t.progressValues
}
