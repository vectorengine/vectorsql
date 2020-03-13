// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package executors

import (
	"fmt"
	"strings"
	"time"

	"columns"
	"datablocks"
	"datastreams"
	"datatypes"
	"datavalues"
	"expressions"
	"planners"
	"processors"
	"transforms"

	"github.com/gammazero/workerpool"
)

type TableValuedFunctionExecutor struct {
	ctx         *ExecutorContext
	plan        *planners.TableValuedFunctionPlan
	duration    time.Duration
	transformer processors.IProcessor
}

func NewTableValuedFunctionExecutor(ctx *ExecutorContext, plan *planners.TableValuedFunctionPlan) IExecutor {
	return &TableValuedFunctionExecutor{
		ctx:  ctx,
		plan: plan,
	}
}

func (executor *TableValuedFunctionExecutor) Execute() (*Result, error) {
	var constants []interface{}
	var variables []datavalues.IDataValue

	plan := executor.plan
	log := executor.ctx.log
	conf := executor.ctx.conf
	queue := make(chan interface{}, 64)

	err := plan.Walk(func(plan planners.IPlan) (bool, error) {
		switch plan := plan.(type) {
		case *planners.ConstantPlan:
			constants = append(constants, datavalues.ToValue(plan.Value))
		case *planners.VariablePlan:
			variables = append(variables, datavalues.ToValue(plan.Value))
		}
		return true, nil
	})
	if err != nil {
		return nil, err
	}

	var cols []*columns.Column
	switch strings.ToUpper(plan.FuncName) {
	case "RANGETABLE", "RANDTABLE":
		for i := 1; i < len(variables); i++ {
			datatype, err := datatypes.DataTypeFactory(datavalues.AsString(constants[i].(datavalues.IDataValue)))
			if err != nil {
				return nil, err
			}
			cols = append(cols, columns.NewColumn(datavalues.AsString(variables[i]), datatype))
		}
	case "LOGMOCK":
		cols = []*columns.Column{
			columns.NewColumn("server", datatypes.NewStringDataType()),
			columns.NewColumn("path", datatypes.NewStringDataType()),
			columns.NewColumn("method", datatypes.NewStringDataType()),
			columns.NewColumn("status", datatypes.NewInt32DataType()),
			columns.NewColumn("response_time", datatypes.NewInt32DataType()),
		}
	}

	rows := int(datavalues.AsInt(constants[0].(datavalues.IDataValue)))
	blocksize := conf.Server.DefaultBlockSize
	chunks := (rows / blocksize)

	go func() {
		defer close(queue)

		workerPool := workerpool.New(conf.Runtime.ParallelWorkerNumber)

		start := time.Now()
		for i := 0; i < chunks+1; i++ {
			begin := i * blocksize
			end := (i + 1) * blocksize
			if end > rows {
				end = rows
			}

			workerPool.Submit(func() {
				block := datablocks.NewDataBlock(cols)

				var consts []interface{}
				consts = append(consts, datavalues.ToValue(begin))
				consts = append(consts, datavalues.ToValue(end))
				consts = append(consts, constants[1:]...)
				expr, err := expressions.ExpressionFactory(plan.FuncName, consts)
				if err != nil {
					log.Error("%+v", err)
					queue <- err
					return
				}
				if err := expr.Eval(); err != nil {
					log.Error("%+v", err)
					queue <- err
					return
				}
				rows := datavalues.AsSlice(expr.Result())
				for _, row := range rows {
					if err := block.WriteRow(datavalues.AsSlice(row)); err != nil {
						queue <- err
						log.Error("%+v", err)
						return
					}
				}
				queue <- block
			})
		}

		workerPool.StopWait()
		executor.duration = time.Since(start)
	}()

	// Stream.
	stream := datastreams.NewChannelBlockInputStream(queue)
	transformCtx := transforms.NewTransformContext(executor.ctx.ctx, executor.ctx.log, executor.ctx.conf)
	transformCtx.SetProgressCallback(executor.ctx.progressCallback)
	transform := transforms.NewDataSourceTransform(transformCtx, stream)
	executor.transformer = transform

	result := NewResult()
	result.SetInput(transform)
	return result, nil
}

func (executor *TableValuedFunctionExecutor) String() string {
	transformer := executor.transformer.(*transforms.DataSourceTransform)
	return fmt.Sprintf("(%v, stats:%+v, cost:%v)", transformer.Name(), transformer.Stats(), executor.duration)
}
