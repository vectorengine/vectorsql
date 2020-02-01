// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package planners

import (
	"testing"

	"parsers"
	"parsers/sqlparser"

	"github.com/stretchr/testify/assert"
)

func TestSelectPlan(t *testing.T) {
	tests := []struct {
		err    error
		name   string
		query  string
		expect string
	}{
		{
			name:  "simple",
			query: "select * from t1",
			expect: `{
    "Name": "SelectPlan",
    "SubPlan": {
        "Name": "MapPlan",
        "SubPlans": [
            {
                "Name": "ScanPlan",
                "Table": "t1",
                "Schema": ""
            },
            {
                "Name": "ProjectPlan",
                "SubPlan": {
                    "Name": "MapPlan"
                }
            },
            {
                "Name": "SinkPlan"
            }
        ]
    }
}`,
		},
		{
			name:  "orderby",
			query: "select * from t1 order by c1 desc, c2 asc, c3 desc",
			expect: `{
    "Name": "SelectPlan",
    "SubPlan": {
        "Name": "MapPlan",
        "SubPlans": [
            {
                "Name": "ScanPlan",
                "Table": "t1",
                "Schema": ""
            },
            {
                "Name": "ProjectPlan",
                "SubPlan": {
                    "Name": "MapPlan"
                }
            },
            {
                "Name": "OrderByPlan",
                "Orders": [
                    {
                        "Expression": {
                            "Name": "VariablePlan",
                            "Value": "c1"
                        },
                        "Direction": "desc"
                    },
                    {
                        "Expression": {
                            "Name": "VariablePlan",
                            "Value": "c2"
                        },
                        "Direction": "asc"
                    },
                    {
                        "Expression": {
                            "Name": "VariablePlan",
                            "Value": "c3"
                        },
                        "Direction": "desc"
                    }
                ]
            },
            {
                "Name": "SinkPlan"
            }
        ]
    }
}`,
		},
		{
			name:  "simple",
			query: "select name, sum(id), (id+1) from system.tables where (name='db1' or name='db2') and (id+1)>3",
			expect: `{
    "Name": "SelectPlan",
    "SubPlan": {
        "Name": "MapPlan",
        "SubPlans": [
            {
                "Name": "ScanPlan",
                "Table": "tables",
                "Schema": "system"
            },
            {
                "Name": "ProjectPlan",
                "SubPlan": {
                    "Name": "MapPlan",
                    "SubPlans": [
                        {
                            "Name": "VariablePlan",
                            "Value": "name"
                        },
                        {
                            "Name": "FunctionExpressionPlan",
                            "FuncName": "SUM",
                            "Args": [
                                {
                                    "Name": "VariablePlan",
                                    "Value": "id"
                                }
                            ]
                        },
                        {
                            "Name": "FunctionExpressionPlan",
                            "FuncName": "+",
                            "Args": [
                                {
                                    "Name": "VariablePlan",
                                    "Value": "id"
                                },
                                {
                                    "Name": "ConstantPlan",
                                    "Value": 1
                                }
                            ]
                        }
                    ]
                }
            },
            {
                "Name": "FilterPlan",
                "SubPlan": {
                    "Name": "AndPlan",
                    "FuncName": "AND",
                    "Left": {
                        "Name": "OrPlan",
                        "FuncName": "OR",
                        "Left": {
                            "Name": "BooleanExpressionPlan",
                            "Args": [
                                {
                                    "Name": "VariablePlan",
                                    "Value": "name"
                                },
                                {
                                    "Name": "ConstantPlan",
                                    "Value": "db1"
                                }
                            ],
                            "FuncName": "="
                        },
                        "Right": {
                            "Name": "BooleanExpressionPlan",
                            "Args": [
                                {
                                    "Name": "VariablePlan",
                                    "Value": "name"
                                },
                                {
                                    "Name": "ConstantPlan",
                                    "Value": "db2"
                                }
                            ],
                            "FuncName": "="
                        }
                    },
                    "Right": {
                        "Name": "BooleanExpressionPlan",
                        "Args": [
                            {
                                "Name": "FunctionExpressionPlan",
                                "FuncName": "+",
                                "Args": [
                                    {
                                        "Name": "VariablePlan",
                                        "Value": "id"
                                    },
                                    {
                                        "Name": "ConstantPlan",
                                        "Value": 1
                                    }
                                ]
                            },
                            {
                                "Name": "ConstantPlan",
                                "Value": 3
                            }
                        ],
                        "FuncName": "\u003e"
                    }
                }
            },
            {
                "Name": "SinkPlan"
            }
        ]
    }
}`,
		},
		{
			name:  "tvf-range",
			query: "SELECT * FROM range(range_start -> 1, range_end -> 5) r",
			expect: `{
    "Name": "SelectPlan",
    "SubPlan": {
        "Name": "MapPlan",
        "SubPlans": [
            {
                "Name": "TableValuedFunctionPlan",
                "As": "",
                "FuncName": "range",
                "SubPlan": {
                    "Name": "MapPlan",
                    "SubPlans": [
                        {
                            "Name": "TableValuedFunctionExpressionPlan",
                            "FuncName": "",
                            "SubPlan": {
                                "Name": "FunctionExpressionPlan",
                                "FuncName": "-\u003e",
                                "Args": [
                                    {
                                        "Name": "VariablePlan",
                                        "Value": "range_start"
                                    },
                                    {
                                        "Name": "ConstantPlan",
                                        "Value": 1
                                    }
                                ]
                            }
                        },
                        {
                            "Name": "TableValuedFunctionExpressionPlan",
                            "FuncName": "",
                            "SubPlan": {
                                "Name": "FunctionExpressionPlan",
                                "FuncName": "-\u003e",
                                "Args": [
                                    {
                                        "Name": "VariablePlan",
                                        "Value": "range_end"
                                    },
                                    {
                                        "Name": "ConstantPlan",
                                        "Value": 5
                                    }
                                ]
                            }
                        }
                    ]
                }
            },
            {
                "Name": "ProjectPlan",
                "SubPlan": {
                    "Name": "MapPlan"
                }
            },
            {
                "Name": "SinkPlan"
            }
        ]
    }
}`,
		},
	}

	for _, test := range tests {
		statement, err := parsers.Parse(test.query)
		assert.Nil(t, err)

		plan := NewSelectPlan(statement.(*sqlparser.Select))
		err = plan.Build()
		assert.Nil(t, err)

		expect := test.expect
		actual := plan.String()
		assert.Equal(t, expect, actual)
	}
}
