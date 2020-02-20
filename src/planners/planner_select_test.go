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
                "Name": "SelectionPlan",
                "Projects": {
                    "Name": "MapPlan"
                },
                "GroupBys": {
                    "Name": "MapPlan"
                },
                "SelectionMode": "NormalSelection"
            },
            {
                "Name": "SinkPlan"
            }
        ]
    }
}`,
		},
		{
			name: "complex",
			query: `SELECT 
    COUNT(server) as count,
    SUM(IF(status != 200, 1, 0)) AS errors, 
    SUM(IF(status = 200, 1, 0)) AS success, 
    errors / COUNT(server) AS error_rate, 
    success / COUNT(server) AS success_rate, 
    SUM(response_time) / COUNT(server) AS load_avg, 
    MIN(response_time), 
    MAX(response_time), 
    server
FROM logmock(rows -> 15)
GROUP BY server
HAVING errors > 0
ORDER BY server ASC, load_avg DESC
LIMIT 10`,
			expect: `{
    "Name": "SelectPlan",
    "SubPlan": {
        "Name": "MapPlan",
        "SubPlans": [
            {
                "Name": "TableValuedFunctionPlan",
                "As": "",
                "FuncName": "logmock",
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
                                    "Value": 15
                                }
                            }
                        }
                    ]
                }
            },
            {
                "Name": "SelectionPlan",
                "Projects": {
                    "Name": "MapPlan",
                    "SubPlans": [
                        {
                            "Name": "AliasedExpressionPlan",
                            "As": "count",
                            "Expr": {
                                "Name": "UnaryExpressionPlan",
                                "FuncName": "COUNT",
                                "Expr": {
                                    "Name": "VariablePlan",
                                    "Value": "server"
                                }
                            }
                        },
                        {
                            "Name": "AliasedExpressionPlan",
                            "As": "errors",
                            "Expr": {
                                "Name": "UnaryExpressionPlan",
                                "FuncName": "SUM",
                                "Expr": {
                                    "Name": "FunctionExpressionPlan",
                                    "FuncName": "IF",
                                    "Args": [
                                        {
                                            "Name": "BinaryExpressionPlan",
                                            "FuncName": "!=",
                                            "Left": {
                                                "Name": "VariablePlan",
                                                "Value": "status"
                                            },
                                            "Right": {
                                                "Name": "ConstantPlan",
                                                "Value": 200
                                            }
                                        },
                                        {
                                            "Name": "ConstantPlan",
                                            "Value": 1
                                        },
                                        {
                                            "Name": "ConstantPlan",
                                            "Value": 0
                                        }
                                    ]
                                }
                            }
                        },
                        {
                            "Name": "AliasedExpressionPlan",
                            "As": "success",
                            "Expr": {
                                "Name": "UnaryExpressionPlan",
                                "FuncName": "SUM",
                                "Expr": {
                                    "Name": "FunctionExpressionPlan",
                                    "FuncName": "IF",
                                    "Args": [
                                        {
                                            "Name": "BinaryExpressionPlan",
                                            "FuncName": "=",
                                            "Left": {
                                                "Name": "VariablePlan",
                                                "Value": "status"
                                            },
                                            "Right": {
                                                "Name": "ConstantPlan",
                                                "Value": 200
                                            }
                                        },
                                        {
                                            "Name": "ConstantPlan",
                                            "Value": 1
                                        },
                                        {
                                            "Name": "ConstantPlan",
                                            "Value": 0
                                        }
                                    ]
                                }
                            }
                        },
                        {
                            "Name": "AliasedExpressionPlan",
                            "As": "error_rate",
                            "Expr": {
                                "Name": "BinaryExpressionPlan",
                                "FuncName": "/",
                                "Left": {
                                    "Name": "UnaryExpressionPlan",
                                    "FuncName": "SUM",
                                    "Expr": {
                                        "Name": "FunctionExpressionPlan",
                                        "FuncName": "IF",
                                        "Args": [
                                            {
                                                "Name": "BinaryExpressionPlan",
                                                "FuncName": "!=",
                                                "Left": {
                                                    "Name": "VariablePlan",
                                                    "Value": "status"
                                                },
                                                "Right": {
                                                    "Name": "ConstantPlan",
                                                    "Value": 200
                                                }
                                            },
                                            {
                                                "Name": "ConstantPlan",
                                                "Value": 1
                                            },
                                            {
                                                "Name": "ConstantPlan",
                                                "Value": 0
                                            }
                                        ]
                                    }
                                },
                                "Right": {
                                    "Name": "UnaryExpressionPlan",
                                    "FuncName": "COUNT",
                                    "Expr": {
                                        "Name": "VariablePlan",
                                        "Value": "server"
                                    }
                                }
                            }
                        },
                        {
                            "Name": "AliasedExpressionPlan",
                            "As": "success_rate",
                            "Expr": {
                                "Name": "BinaryExpressionPlan",
                                "FuncName": "/",
                                "Left": {
                                    "Name": "UnaryExpressionPlan",
                                    "FuncName": "SUM",
                                    "Expr": {
                                        "Name": "FunctionExpressionPlan",
                                        "FuncName": "IF",
                                        "Args": [
                                            {
                                                "Name": "BinaryExpressionPlan",
                                                "FuncName": "=",
                                                "Left": {
                                                    "Name": "VariablePlan",
                                                    "Value": "status"
                                                },
                                                "Right": {
                                                    "Name": "ConstantPlan",
                                                    "Value": 200
                                                }
                                            },
                                            {
                                                "Name": "ConstantPlan",
                                                "Value": 1
                                            },
                                            {
                                                "Name": "ConstantPlan",
                                                "Value": 0
                                            }
                                        ]
                                    }
                                },
                                "Right": {
                                    "Name": "UnaryExpressionPlan",
                                    "FuncName": "COUNT",
                                    "Expr": {
                                        "Name": "VariablePlan",
                                        "Value": "server"
                                    }
                                }
                            }
                        },
                        {
                            "Name": "AliasedExpressionPlan",
                            "As": "load_avg",
                            "Expr": {
                                "Name": "BinaryExpressionPlan",
                                "FuncName": "/",
                                "Left": {
                                    "Name": "UnaryExpressionPlan",
                                    "FuncName": "SUM",
                                    "Expr": {
                                        "Name": "VariablePlan",
                                        "Value": "response_time"
                                    }
                                },
                                "Right": {
                                    "Name": "UnaryExpressionPlan",
                                    "FuncName": "COUNT",
                                    "Expr": {
                                        "Name": "VariablePlan",
                                        "Value": "server"
                                    }
                                }
                            }
                        },
                        {
                            "Name": "UnaryExpressionPlan",
                            "FuncName": "MIN",
                            "Expr": {
                                "Name": "VariablePlan",
                                "Value": "response_time"
                            }
                        },
                        {
                            "Name": "UnaryExpressionPlan",
                            "FuncName": "MAX",
                            "Expr": {
                                "Name": "VariablePlan",
                                "Value": "response_time"
                            }
                        },
                        {
                            "Name": "VariablePlan",
                            "Value": "server"
                        }
                    ]
                },
                "GroupBys": {
                    "Name": "MapPlan",
                    "SubPlans": [
                        {
                            "Name": "VariablePlan",
                            "Value": "server"
                        }
                    ]
                },
                "SelectionMode": "GroupBySelection"
            },
            {
                "Name": "FilterPlan",
                "SubPlan": {
                    "Name": "BinaryExpressionPlan",
                    "FuncName": "\u003e",
                    "Left": {
                        "Name": "VariablePlan",
                        "Value": "errors"
                    },
                    "Right": {
                        "Name": "ConstantPlan",
                        "Value": 0
                    }
                }
            },
            {
                "Name": "OrderByPlan",
                "Orders": [
                    {
                        "Expression": {
                            "Name": "VariablePlan",
                            "Value": "server"
                        },
                        "Direction": "asc"
                    },
                    {
                        "Expression": {
                            "Name": "VariablePlan",
                            "Value": "load_avg"
                        },
                        "Direction": "desc"
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
                            "Name": "AliasedExpressionPlan",
                            "As": "count",
                            "Expr": {
                                "Name": "UnaryExpressionPlan",
                                "FuncName": "COUNT",
                                "Expr": {
                                    "Name": "VariablePlan",
                                    "Value": "server"
                                }
                            }
                        },
                        {
                            "Name": "AliasedExpressionPlan",
                            "As": "errors",
                            "Expr": {
                                "Name": "UnaryExpressionPlan",
                                "FuncName": "SUM",
                                "Expr": {
                                    "Name": "FunctionExpressionPlan",
                                    "FuncName": "IF",
                                    "Args": [
                                        {
                                            "Name": "BinaryExpressionPlan",
                                            "FuncName": "!=",
                                            "Left": {
                                                "Name": "VariablePlan",
                                                "Value": "status"
                                            },
                                            "Right": {
                                                "Name": "ConstantPlan",
                                                "Value": 200
                                            }
                                        },
                                        {
                                            "Name": "ConstantPlan",
                                            "Value": 1
                                        },
                                        {
                                            "Name": "ConstantPlan",
                                            "Value": 0
                                        }
                                    ]
                                }
                            }
                        },
                        {
                            "Name": "AliasedExpressionPlan",
                            "As": "success",
                            "Expr": {
                                "Name": "UnaryExpressionPlan",
                                "FuncName": "SUM",
                                "Expr": {
                                    "Name": "FunctionExpressionPlan",
                                    "FuncName": "IF",
                                    "Args": [
                                        {
                                            "Name": "BinaryExpressionPlan",
                                            "FuncName": "=",
                                            "Left": {
                                                "Name": "VariablePlan",
                                                "Value": "status"
                                            },
                                            "Right": {
                                                "Name": "ConstantPlan",
                                                "Value": 200
                                            }
                                        },
                                        {
                                            "Name": "ConstantPlan",
                                            "Value": 1
                                        },
                                        {
                                            "Name": "ConstantPlan",
                                            "Value": 0
                                        }
                                    ]
                                }
                            }
                        },
                        {
                            "Name": "AliasedExpressionPlan",
                            "As": "error_rate",
                            "Expr": {
                                "Name": "BinaryExpressionPlan",
                                "FuncName": "/",
                                "Left": {
                                    "Name": "UnaryExpressionPlan",
                                    "FuncName": "SUM",
                                    "Expr": {
                                        "Name": "FunctionExpressionPlan",
                                        "FuncName": "IF",
                                        "Args": [
                                            {
                                                "Name": "BinaryExpressionPlan",
                                                "FuncName": "!=",
                                                "Left": {
                                                    "Name": "VariablePlan",
                                                    "Value": "status"
                                                },
                                                "Right": {
                                                    "Name": "ConstantPlan",
                                                    "Value": 200
                                                }
                                            },
                                            {
                                                "Name": "ConstantPlan",
                                                "Value": 1
                                            },
                                            {
                                                "Name": "ConstantPlan",
                                                "Value": 0
                                            }
                                        ]
                                    }
                                },
                                "Right": {
                                    "Name": "UnaryExpressionPlan",
                                    "FuncName": "COUNT",
                                    "Expr": {
                                        "Name": "VariablePlan",
                                        "Value": "server"
                                    }
                                }
                            }
                        },
                        {
                            "Name": "AliasedExpressionPlan",
                            "As": "success_rate",
                            "Expr": {
                                "Name": "BinaryExpressionPlan",
                                "FuncName": "/",
                                "Left": {
                                    "Name": "UnaryExpressionPlan",
                                    "FuncName": "SUM",
                                    "Expr": {
                                        "Name": "FunctionExpressionPlan",
                                        "FuncName": "IF",
                                        "Args": [
                                            {
                                                "Name": "BinaryExpressionPlan",
                                                "FuncName": "=",
                                                "Left": {
                                                    "Name": "VariablePlan",
                                                    "Value": "status"
                                                },
                                                "Right": {
                                                    "Name": "ConstantPlan",
                                                    "Value": 200
                                                }
                                            },
                                            {
                                                "Name": "ConstantPlan",
                                                "Value": 1
                                            },
                                            {
                                                "Name": "ConstantPlan",
                                                "Value": 0
                                            }
                                        ]
                                    }
                                },
                                "Right": {
                                    "Name": "UnaryExpressionPlan",
                                    "FuncName": "COUNT",
                                    "Expr": {
                                        "Name": "VariablePlan",
                                        "Value": "server"
                                    }
                                }
                            }
                        },
                        {
                            "Name": "AliasedExpressionPlan",
                            "As": "load_avg",
                            "Expr": {
                                "Name": "BinaryExpressionPlan",
                                "FuncName": "/",
                                "Left": {
                                    "Name": "UnaryExpressionPlan",
                                    "FuncName": "SUM",
                                    "Expr": {
                                        "Name": "VariablePlan",
                                        "Value": "response_time"
                                    }
                                },
                                "Right": {
                                    "Name": "UnaryExpressionPlan",
                                    "FuncName": "COUNT",
                                    "Expr": {
                                        "Name": "VariablePlan",
                                        "Value": "server"
                                    }
                                }
                            }
                        },
                        {
                            "Name": "UnaryExpressionPlan",
                            "FuncName": "MIN",
                            "Expr": {
                                "Name": "VariablePlan",
                                "Value": "response_time"
                            }
                        },
                        {
                            "Name": "UnaryExpressionPlan",
                            "FuncName": "MAX",
                            "Expr": {
                                "Name": "VariablePlan",
                                "Value": "response_time"
                            }
                        },
                        {
                            "Name": "VariablePlan",
                            "Value": "server"
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
