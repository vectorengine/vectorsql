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

func (executor *TableValuedFunctionExecutor) Name() string {
	return "TableValuedFunctionExecutor"
}

func (executor *TableValuedFunctionExecutor) Execute() (processors.IProcessor, error) {
	var args []datatypes.Value
	plan := executor.plan
	log := executor.ctx.log

	log.Debug("Executor->Enter->LogicalPlan:%s", executor.plan)
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
		batcher := datablocks.NewBatchWriter(block.Columns())

		slice := result.AsSlice()
		for _, data := range slice {
			if err := batcher.WriteRow(data); err != nil {
				return nil, err
			}
		}
		if err := block.Write(batcher); err != nil {
			return nil, err
		}
	}
	// Stream.
	stream := datastreams.NewNativeBlockInputStream()
	if err := stream.Insert(block); err != nil {
		return nil, err
	}

	transformCtx := transforms.NewTransformContext(executor.ctx.log, executor.ctx.conf)
	transform := transforms.NewDataSourceTransform(transformCtx, stream)
	log.Debug("Executor->Return->Pipeline:%s", transform.Name())
	return transform, nil
}

func (executor *TableValuedFunctionExecutor) String() string {
	res := "\n"
	res += "->"
	res += executor.Name()
	res += "\t--> "
	res += executor.plan.String()
	return res
}
