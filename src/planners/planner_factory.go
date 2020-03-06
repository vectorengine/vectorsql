// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package planners

import (
	"parsers"

	"base/errors"
	"parsers/sqlparser"
)

type planCreator func(ast sqlparser.Statement) IPlan

var table = map[string]planCreator{
	sqlparser.NodeNameUse:            NewUsePlan,
	sqlparser.NodeNameSelect:         NewSelectPlan,
	sqlparser.NodeNameDatabaseCreate: NewCreateDatabasePlan,
	sqlparser.NodeNameDatabaseDrop:   NewDropDatabasePlan,
	sqlparser.NodeNameTableCreate:    NewCreateTablePlan,
	sqlparser.NodeNameTableDrop:      NewDropTablePlan,
	sqlparser.NodeNameShowDatabases:  NewShowDatabasesPlan,
	sqlparser.NodeNameShowTables:     NewShowTablesPlan,
	sqlparser.NodeNameInsert:         NewInsertPlan,
}

func PlanFactory(query string) (IPlan, error) {
	statement, err := parsers.Parse(query)
	if err != nil {
		return nil, err
	}

	creator, ok := table[statement.Name()]
	if !ok {
		return nil, errors.Errorf("Couldn't get the planner:%T", statement)
	}
	plan := creator(statement)
	return plan, plan.Build()
}
