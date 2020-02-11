// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package executors

import (
	"parsers"
	"parsers/sqlparser"
	"planners"
)

func NewShowTablesExecutor(ctx *ExecutorContext, plan planners.IPlan) IExecutor {
	var (
		planner = plan.(*planners.ShowTablesPlan)
		opt     = planner.GetAst().ShowTablesOpt
		db      = ctx.session.GetDatabase()
		buffer  = sqlparser.NewTrackedBuffer(nil)
		ast     = planner.GetAst()
	)

	buffer.Myprintf("%s", "select name from system.tables where")
	if opt != nil && opt.DbName != "" {
		db = opt.DbName
	}
	buffer.Myprintf(" `database` = '%s'", db)
	if opt != nil && opt.Filter != nil {
		if opt.Filter.Like != "" {
			not := " "
			if opt.Filter.Not {
				not = " not "
			}
			buffer.Myprintf(" and name%slike '%s'", not, opt.Filter.Like)
		} else if opt.Filter.Filter != nil {
			buffer.Myprintf(" and (%v)", opt.Filter.Filter)
		}
	}

	buffer.Myprintf(" order by name asc")
	if ast.Limit != nil {
		ast.Limit.Format(buffer)
	}

	newAst, err := parsers.Parse(buffer.String())
	if err != nil {
		ctx.log.Error("Error show tables query %v", err)
	}
	planner.SubPlan = planners.NewSelectPlan(newAst)
	planner.SubPlan.Build()

	return NewSelectExecutor(ctx, planner.SubPlan)
}
