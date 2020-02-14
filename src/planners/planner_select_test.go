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
                "Name": "GroupByPlan",
                "HasAggregate": false,
                "Projects": {
                    "Name": "MapPlan"
                },
                "GroupBys": {
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
			name:  "filter",
			query: "select sum(c1+1) as sum1, c1, c2 from randtable(rows->10, c1->'UInt32', c2->'UInt32', c3->'String') where c1>1 and c2<10 group by c3 order by c3 desc, sum1 desc",
			expect: `{
    "Name": "SelectPlan",
    "SubPlan": {
        "Name": "MapPlan",
        "SubPlans": [
            {
                "Name": "TableValuedFunctionPlan",
                "As": "",
                "FuncName": "randtable",
                "SubPlan": {
                    "Name": "MapPlan",
                    "SubPlans": [
                        {
                            "Name": "TableValuedFunctionExpressionPlan",
                            "FuncName": "",
                            "SubPlan": {
                                "Name": "BinaryExpressionPlan",
                                "FuncName": "-\u003e",
                                "Left": {
                                    "Name": "VariablePlan",
                                    "Value": "rows"
                                },
                                "Right": {
                                    "Name": "ConstantPlan",
                                    "Value": 10
                                }
                            }
                        },
                        {
                            "Name": "TableValuedFunctionExpressionPlan",
                            "FuncName": "",
                            "SubPlan": {
                                "Name": "BinaryExpressionPlan",
                                "FuncName": "-\u003e",
                                "Left": {
                                    "Name": "VariablePlan",
                                    "Value": "c1"
                                },
                                "Right": {
                                    "Name": "ConstantPlan",
                                    "Value": "UInt32"
                                }
                            }
                        },
                        {
                            "Name": "TableValuedFunctionExpressionPlan",
                            "FuncName": "",
                            "SubPlan": {
                                "Name": "BinaryExpressionPlan",
                                "FuncName": "-\u003e",
                                "Left": {
                                    "Name": "VariablePlan",
                                    "Value": "c2"
                                },
                                "Right": {
                                    "Name": "ConstantPlan",
                                    "Value": "UInt32"
                                }
                            }
                        },
                        {
                            "Name": "TableValuedFunctionExpressionPlan",
                            "FuncName": "",
                            "SubPlan": {
                                "Name": "BinaryExpressionPlan",
                                "FuncName": "-\u003e",
                                "Left": {
                                    "Name": "VariablePlan",
                                    "Value": "c3"
                                },
                                "Right": {
                                    "Name": "ConstantPlan",
                                    "Value": "String"
                                }
                            }
                        }
                    ]
                }
            },
            {
                "Name": "FilterPlan",
                "SubPlan": {
                    "Name": "BinaryExpressionPlan",
                    "FuncName": "AND",
                    "Left": {
                        "Name": "BinaryExpressionPlan",
                        "FuncName": "\u003e",
                        "Left": {
                            "Name": "VariablePlan",
                            "Value": "c1"
                        },
                        "Right": {
                            "Name": "ConstantPlan",
                            "Value": 1
                        }
                    },
                    "Right": {
                        "Name": "BinaryExpressionPlan",
                        "FuncName": "\u003c",
                        "Left": {
                            "Name": "VariablePlan",
                            "Value": "c2"
                        },
                        "Right": {
                            "Name": "ConstantPlan",
                            "Value": 10
                        }
                    }
                }
            },
            {
                "Name": "GroupByPlan",
                "HasAggregate": false,
                "Projects": {
                    "Name": "MapPlan",
                    "SubPlans": [
                        {
                            "Name": "AliasedExpressionPlan",
                            "As": "sum1",
                            "Expr": {
                                "Name": "UnaryExpressionPlan",
                                "FuncName": "SUM",
                                "Expr": {
                                    "Name": "BinaryExpressionPlan",
                                    "FuncName": "+",
                                    "Left": {
                                        "Name": "VariablePlan",
                                        "Value": "c1"
                                    },
                                    "Right": {
                                        "Name": "ConstantPlan",
                                        "Value": 1
                                    }
                                }
                            }
                        },
                        {
                            "Name": "VariablePlan",
                            "Value": "c1"
                        },
                        {
                            "Name": "VariablePlan",
                            "Value": "c2"
                        }
                    ]
                },
                "GroupBys": {
                    "Name": "MapPlan",
                    "SubPlans": [
                        {
                            "Name": "VariablePlan",
                            "Value": "c3"
                        }
                    ]
                }
            },
            {
                "Name": "OrderByPlan",
                "Orders": [
                    {
                        "Expression": {
                            "Name": "VariablePlan",
                            "Value": "c3"
                        },
                        "Direction": "desc"
                    },
                    {
                        "Expression": {
                            "Name": "VariablePlan",
                            "Value": "sum1"
                        },
                        "Direction": "desc"
                    }
                ]
            },
            {
                "Name": "ProjectionPlan",
                "Projections": {
                    "Name": "MapPlan",
                    "SubPlans": [
                        {
                            "Name": "AliasedExpressionPlan",
                            "As": "sum1",
                            "Expr": {
                                "Name": "UnaryExpressionPlan",
                                "FuncName": "SUM",
                                "Expr": {
                                    "Name": "BinaryExpressionPlan",
                                    "FuncName": "+",
                                    "Left": {
                                        "Name": "VariablePlan",
                                        "Value": "c1"
                                    },
                                    "Right": {
                                        "Name": "ConstantPlan",
                                        "Value": 1
                                    }
                                }
                            }
                        },
                        {
                            "Name": "VariablePlan",
                            "Value": "c1"
                        },
                        {
                            "Name": "VariablePlan",
                            "Value": "c2"
                        }
                    ]
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
