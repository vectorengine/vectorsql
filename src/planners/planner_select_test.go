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
			name: "filter",
			query: `SELECT 
    IF(c1>10, c2, c2),
    sum(c1) AS c1_sum, 
    count(c1) AS c1_count, 
    c1_sum / c1_count AS c1_avg, 
    c2, 
    c2+1,
    c3
FROM randtable(rows -> 1000, c1 -> 'UInt32', c2 -> 'UInt32', c3 -> 'String')
WHERE (c1 > 80) AND ((c1 + c2) < 500) OR (c2>10)
GROUP BY c3
ORDER BY 
    c1_count DESC, 
    c3 ASC
LIMIT 10
`,
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
                                    "Value": 1000
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
                    "FuncName": "OR",
                    "Left": {
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
                                "Value": 80
                            }
                        },
                        "Right": {
                            "Name": "BinaryExpressionPlan",
                            "FuncName": "\u003c",
                            "Left": {
                                "Name": "BinaryExpressionPlan",
                                "FuncName": "+",
                                "Left": {
                                    "Name": "VariablePlan",
                                    "Value": "c1"
                                },
                                "Right": {
                                    "Name": "VariablePlan",
                                    "Value": "c2"
                                }
                            },
                            "Right": {
                                "Name": "ConstantPlan",
                                "Value": 500
                            }
                        }
                    },
                    "Right": {
                        "Name": "BinaryExpressionPlan",
                        "FuncName": "\u003e",
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
                "HasAggregate": true,
                "Projects": {
                    "Name": "MapPlan",
                    "SubPlans": [
                        {
                            "Name": "FunctionExpressionPlan",
                            "FuncName": "IF",
                            "Args": [
                                {
                                    "Name": "BinaryExpressionPlan",
                                    "FuncName": "\u003e",
                                    "Left": {
                                        "Name": "VariablePlan",
                                        "Value": "c1"
                                    },
                                    "Right": {
                                        "Name": "ConstantPlan",
                                        "Value": 10
                                    }
                                },
                                {
                                    "Name": "VariablePlan",
                                    "Value": "c2"
                                },
                                {
                                    "Name": "VariablePlan",
                                    "Value": "c2"
                                }
                            ]
                        },
                        {
                            "Name": "AliasedExpressionPlan",
                            "As": "c1_sum",
                            "Expr": {
                                "Name": "UnaryExpressionPlan",
                                "FuncName": "SUM",
                                "Expr": {
                                    "Name": "VariablePlan",
                                    "Value": "c1"
                                }
                            }
                        },
                        {
                            "Name": "AliasedExpressionPlan",
                            "As": "c1_count",
                            "Expr": {
                                "Name": "UnaryExpressionPlan",
                                "FuncName": "COUNT",
                                "Expr": {
                                    "Name": "VariablePlan",
                                    "Value": "c1"
                                }
                            }
                        },
                        {
                            "Name": "AliasedExpressionPlan",
                            "As": "c1_avg",
                            "Expr": {
                                "Name": "BinaryExpressionPlan",
                                "FuncName": "/",
                                "Left": {
                                    "Name": "UnaryExpressionPlan",
                                    "FuncName": "SUM",
                                    "Expr": {
                                        "Name": "VariablePlan",
                                        "Value": "c1"
                                    }
                                },
                                "Right": {
                                    "Name": "UnaryExpressionPlan",
                                    "FuncName": "COUNT",
                                    "Expr": {
                                        "Name": "VariablePlan",
                                        "Value": "c1"
                                    }
                                }
                            }
                        },
                        {
                            "Name": "VariablePlan",
                            "Value": "c2"
                        },
                        {
                            "Name": "BinaryExpressionPlan",
                            "FuncName": "+",
                            "Left": {
                                "Name": "VariablePlan",
                                "Value": "c2"
                            },
                            "Right": {
                                "Name": "ConstantPlan",
                                "Value": 1
                            }
                        },
                        {
                            "Name": "VariablePlan",
                            "Value": "c3"
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
                            "Value": "c1_count"
                        },
                        "Direction": "desc"
                    },
                    {
                        "Expression": {
                            "Name": "VariablePlan",
                            "Value": "c3"
                        },
                        "Direction": "asc"
                    }
                ]
            },
            {
                "Name": "LimitPlan",
                "OffsetPlan": {
                    "Name": "ConstantPlan",
                    "Value": 0
                },
                "RowcountPlan": {
                    "Name": "ConstantPlan",
                    "Value": 10
                }
            },
            {
                "Name": "ProjectionPlan",
                "Projections": {
                    "Name": "MapPlan",
                    "SubPlans": [
                        {
                            "Name": "FunctionExpressionPlan",
                            "FuncName": "IF",
                            "Args": [
                                {
                                    "Name": "BinaryExpressionPlan",
                                    "FuncName": "\u003e",
                                    "Left": {
                                        "Name": "VariablePlan",
                                        "Value": "c1"
                                    },
                                    "Right": {
                                        "Name": "ConstantPlan",
                                        "Value": 10
                                    }
                                },
                                {
                                    "Name": "VariablePlan",
                                    "Value": "c2"
                                },
                                {
                                    "Name": "VariablePlan",
                                    "Value": "c2"
                                }
                            ]
                        },
                        {
                            "Name": "AliasedExpressionPlan",
                            "As": "c1_sum",
                            "Expr": {
                                "Name": "UnaryExpressionPlan",
                                "FuncName": "SUM",
                                "Expr": {
                                    "Name": "VariablePlan",
                                    "Value": "c1"
                                }
                            }
                        },
                        {
                            "Name": "AliasedExpressionPlan",
                            "As": "c1_count",
                            "Expr": {
                                "Name": "UnaryExpressionPlan",
                                "FuncName": "COUNT",
                                "Expr": {
                                    "Name": "VariablePlan",
                                    "Value": "c1"
                                }
                            }
                        },
                        {
                            "Name": "AliasedExpressionPlan",
                            "As": "c1_avg",
                            "Expr": {
                                "Name": "BinaryExpressionPlan",
                                "FuncName": "/",
                                "Left": {
                                    "Name": "UnaryExpressionPlan",
                                    "FuncName": "SUM",
                                    "Expr": {
                                        "Name": "VariablePlan",
                                        "Value": "c1"
                                    }
                                },
                                "Right": {
                                    "Name": "UnaryExpressionPlan",
                                    "FuncName": "COUNT",
                                    "Expr": {
                                        "Name": "VariablePlan",
                                        "Value": "c1"
                                    }
                                }
                            }
                        },
                        {
                            "Name": "VariablePlan",
                            "Value": "c2"
                        },
                        {
                            "Name": "BinaryExpressionPlan",
                            "FuncName": "+",
                            "Left": {
                                "Name": "VariablePlan",
                                "Value": "c2"
                            },
                            "Right": {
                                "Name": "ConstantPlan",
                                "Value": 1
                            }
                        },
                        {
                            "Name": "VariablePlan",
                            "Value": "c3"
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
