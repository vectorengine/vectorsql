// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package executors

import (
	"columns"
	"datablocks"
	"datastreams"
	"datatypes"
	"functions"
	"planners"
	"processors"
	"transforms"
)

type TableValuedFunctionExecutor struct {
	ctx  *ExecutorContext
	plan *planners.TableValuedFunctionPlan
}

func NewTableValuedFunctionExecutor(ctx *ExecutorContext, plan *planners.TableValuedFunctionPlan) *TableValuedFunctionExecutor {
	return &TableValuedFunctionExecutor{
		ctx:  ctx,
		plan: plan,
	}
}

func (executor *TableValuedFunctionExecutor) Execute() (processors.IProcessor, error) {
	plan := executor.plan
	var args []datatypes.Value

	err := plan.Walk(func(plan planners.IPlan) (bool, error) {
		switch plan := plan.(type) {
		case *planners.ConstantPlan:
			args = append(args, datatypes.ToValue(plan.Value))
		}
		return true, nil
	})
	if err != nil {
		return nil, err
	}

	function, err := functions.FunctionFactory(plan.FuncName)
	if err != nil {
		return nil, err
	}
	if err := function.Validator.Validate(args...); err != nil {
		return nil, err
	}
	result, err := function.Logic(args...)
	if err != nil {
		return nil, err
	}

	var block *datablocks.DataBlock
	switch plan.FuncName {
	case "range":
		block = datablocks.NewDataBlock([]columns.Column{
			{Name: "i", DataType: datatypes.NewInt32DataType()},
		})
		slice := result.AsSlice()
		for _, data := range slice {
			if err := block.ColumnValues()[0].Insert(data); err != nil {
				return nil, err
			}
		}
	case "arrayjoin":
	}
	// Stream.
	stream := datastreams.NewNativeBlockInputStream()
	if err := stream.Insert(block); err != nil {
		return nil, err
	}

	transformCtx := transforms.NewTransformContext(executor.ctx.log, executor.ctx.conf)
	return transforms.NewDataSourceTransform(transformCtx, stream), nil
}

func (executor *TableValuedFunctionExecutor) Name() string {
	return "TableValuedFunctionExecutor"
}

func (executor *TableValuedFunctionExecutor) String() string {
	return "TableValuedFunctionExecutor"
}
