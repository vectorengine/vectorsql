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
	var constants []*datatypes.Value
	var variables []*datatypes.Value

	plan := executor.plan
	log := executor.ctx.log

	log.Debug("Executor->Enter->LogicalPlan:%s", executor.plan)
	err := plan.Walk(func(plan planners.IPlan) (bool, error) {
		switch plan := plan.(type) {
		case *planners.ConstantPlan:
			constants = append(constants, datatypes.ToValue(plan.Value))
		case *planners.VariablePlan:
			variables = append(variables, datatypes.ToValue(plan.Value))
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
	if err := function.Validator.Validate(constants...); err != nil {
		return nil, err
	}
	result, err := function.Logic(constants...)
	if err != nil {
		return nil, err
	}

	var block *datablocks.DataBlock
	switch plan.FuncName {
	case "range":
		block = datablocks.NewDataBlock([]columns.Column{
			{Name: "i", DataType: datatypes.NewInt32DataType()},
		})
	case "rangetable", "randtable":
		var cols []columns.Column
		for i := 1; i < len(variables); i++ {
			datatype, err := datatypes.DataTypeFactory(constants[i].AsString())
			if err != nil {
				return nil, err
			}
			cols = append(cols, columns.Column{
				Name:     variables[i].AsString(),
				DataType: datatype,
			})
		}
		block = datablocks.NewDataBlock(cols)
	}

	// Block.
	batcher := datablocks.NewBatchWriter(block.Columns())
	slice := result.AsSlice()
	for _, data := range slice {
		if err := batcher.WriteRow(data.AsSlice()...); err != nil {
			return nil, err
		}
	}
	if err := block.Write(batcher); err != nil {
		return nil, err
	}

	// Stream.
	stream := datastreams.NewOneBlockInputStream(block)
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
