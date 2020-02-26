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
)

type TableValuedFunctionExecutor struct {
	ctx         *ExecutorContext
	plan        *planners.TableValuedFunctionPlan
	duration    time.Duration
	transformer processors.IProcessor
}

func NewTableValuedFunctionExecutor(ctx *ExecutorContext, plan *planners.TableValuedFunctionPlan) *TableValuedFunctionExecutor {
	return &TableValuedFunctionExecutor{
		ctx:  ctx,
		plan: plan,
	}
}

func (executor *TableValuedFunctionExecutor) Execute() (processors.IProcessor, error) {
	var constants []interface{}
	var variables []datavalues.IDataValue

	plan := executor.plan
	log := executor.ctx.log
	conf := executor.ctx.conf

	log.Debug("Executor->Enter->LogicalPlan:%s", executor.plan)
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

	start := time.Now()
	function, err := expressions.ExpressionFactory(plan.FuncName, constants)
	if err != nil {
		return nil, err
	}
	result, err := function.Update(nil)
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

	// Block.
	var blocks []*datablocks.DataBlock
	slice := datavalues.AsSlice(result)
	slicesize := len(slice)
	blocksize := conf.Server.DefaultBlockSize
	chunks := (slicesize / blocksize)
	for i := 0; i < chunks+1; i++ {
		block := datablocks.NewDataBlock(cols)

		begin := i * blocksize
		end := (i + 1) * blocksize
		if end > slicesize {
			end = slicesize
		}
		for j := begin; j < end; j++ {
			if err := block.WriteRow(datavalues.AsSlice(slice[j])); err != nil {
				return nil, err
			}
		}
		blocks = append(blocks, block)
	}
	executor.duration = time.Since(start)

	// Stream.
	stream := datastreams.NewOneBlockInputStream(blocks...)
	transformCtx := transforms.NewTransformContext(executor.ctx.ctx, executor.ctx.log, executor.ctx.conf)
	transform := transforms.NewDataSourceTransform(transformCtx, stream)
	executor.transformer = transform
	log.Debug("Executor->Return->Pipeline:%s", transform.Name())
	return transform, nil
}

func (executor *TableValuedFunctionExecutor) String() string {
	return fmt.Sprintf("(%v, cost:%v)", executor.transformer.Name(), executor.duration)
}
