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
			name:  "filter",
			query: "select * from t1 where (id>1 and id<8) or id=9 and c>2019.12 or d='name'",
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
                "Name": "FilterPlan",
                "SubPlan": {
                    "Name": "OrPlan",
                    "FuncName": "OR",
                    "Left": {
                        "Name": "OrPlan",
                        "FuncName": "OR",
                        "Left": {
                            "Name": "AndPlan",
                            "FuncName": "AND",
                            "Left": {
                                "Name": "BooleanExpressionPlan",
                                "Args": [
                                    {
                                        "Name": "VariablePlan",
                                        "Value": "id"
                                    },
                                    {
                                        "Name": "ConstantPlan",
                                        "Value": 1
                                    }
                                ],
                                "FuncName": "\u003e"
                            },
                            "Right": {
                                "Name": "BooleanExpressionPlan",
                                "Args": [
                                    {
                                        "Name": "VariablePlan",
                                        "Value": "id"
                                    },
                                    {
                                        "Name": "ConstantPlan",
                                        "Value": 8
                                    }
                                ],
                                "FuncName": "\u003c"
                            }
                        },
                        "Right": {
                            "Name": "AndPlan",
                            "FuncName": "AND",
                            "Left": {
                                "Name": "BooleanExpressionPlan",
                                "Args": [
                                    {
                                        "Name": "VariablePlan",
                                        "Value": "id"
                                    },
                                    {
                                        "Name": "ConstantPlan",
                                        "Value": 9
                                    }
                                ],
                                "FuncName": "="
                            },
                            "Right": {
                                "Name": "BooleanExpressionPlan",
                                "Args": [
                                    {
                                        "Name": "VariablePlan",
                                        "Value": "c"
                                    },
                                    {
                                        "Name": "ConstantPlan",
                                        "Value": 2019.12
                                    }
                                ],
                                "FuncName": "\u003e"
                            }
                        }
                    },
                    "Right": {
                        "Name": "BooleanExpressionPlan",
                        "Args": [
                            {
                                "Name": "VariablePlan",
                                "Value": "d"
                            },
                            {
                                "Name": "ConstantPlan",
                                "Value": "name"
                            }
                        ],
                        "FuncName": "="
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
		{
			name:  "select-aggregate",
			query: "SELECT max(a), sum(b), c, (id+1) FROM t1 where (id+1)!=2 group by d,e order by c desc",
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
                    "Name": "MapPlan",
                    "SubPlans": [
                        {
                            "Name": "FunctionExpressionPlan",
                            "FuncName": "MAX",
                            "Args": [
                                {
                                    "Name": "VariablePlan",
                                    "Value": "a"
                                }
                            ]
                        },
                        {
                            "Name": "FunctionExpressionPlan",
                            "FuncName": "SUM",
                            "Args": [
                                {
                                    "Name": "VariablePlan",
                                    "Value": "b"
                                }
                            ]
                        },
                        {
                            "Name": "VariablePlan",
                            "Value": "c"
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
                            "Value": 2
                        }
                    ],
                    "FuncName": "!="
                }
            },
            {
                "Name": "GroupByPlan",
                "Projects": {
                    "Name": "MapPlan",
                    "SubPlans": [
                        {
                            "Name": "FunctionExpressionPlan",
                            "FuncName": "MAX",
                            "Args": [
                                {
                                    "Name": "VariablePlan",
                                    "Value": "a"
                                }
                            ]
                        },
                        {
                            "Name": "FunctionExpressionPlan",
                            "FuncName": "SUM",
                            "Args": [
                                {
                                    "Name": "VariablePlan",
                                    "Value": "b"
                                }
                            ]
                        },
                        {
                            "Name": "VariablePlan",
                            "Value": "c"
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
                },
                "GroupBys": {
                    "Name": "MapPlan",
                    "SubPlans": [
                        {
                            "Name": "VariablePlan",
                            "Value": "d"
                        },
                        {
                            "Name": "VariablePlan",
                            "Value": "e"
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
                            "Value": "c"
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
			name:  "select-test",
			query: "SELECT max(a+1), (id+1) as b FROM t1 group by a, b",
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
                    "Name": "MapPlan",
                    "SubPlans": [
                        {
                            "Name": "FunctionExpressionPlan",
                            "FuncName": "MAX",
                            "Args": [
                                {
                                    "Name": "FunctionExpressionPlan",
                                    "FuncName": "+",
                                    "Args": [
                                        {
                                            "Name": "VariablePlan",
                                            "Value": "a"
                                        },
                                        {
                                            "Name": "ConstantPlan",
                                            "Value": 1
                                        }
                                    ]
                                }
                            ]
                        },
                        {
                            "Name": "AliasedExpressionPlan",
                            "As": "b",
                            "Expr": {
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
                        }
                    ]
                }
            },
            {
                "Name": "GroupByPlan",
                "Projects": {
                    "Name": "MapPlan",
                    "SubPlans": [
                        {
                            "Name": "FunctionExpressionPlan",
                            "FuncName": "MAX",
                            "Args": [
                                {
                                    "Name": "FunctionExpressionPlan",
                                    "FuncName": "+",
                                    "Args": [
                                        {
                                            "Name": "VariablePlan",
                                            "Value": "a"
                                        },
                                        {
                                            "Name": "ConstantPlan",
                                            "Value": 1
                                        }
                                    ]
                                }
                            ]
                        },
                        {
                            "Name": "AliasedExpressionPlan",
                            "As": "b",
                            "Expr": {
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
                        }
                    ]
                },
                "GroupBys": {
                    "Name": "MapPlan",
                    "SubPlans": [
                        {
                            "Name": "VariablePlan",
                            "Value": "a"
                        },
                        {
                            "Name": "AliasedExpressionPlan",
                            "As": "b",
                            "Expr": {
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
