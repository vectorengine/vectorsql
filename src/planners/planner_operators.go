package planners

type Operator string

const (
	OperatorEqual        Operator = "="
	OperatorNotEqual     Operator = "!="
	OperatorMoreThan     Operator = ">"
	OperatorLessThan     Operator = "<"
	OperatorLike         Operator = "like"
	OperatorIn           Operator = "in"
	OperatorNotIn        Operator = "not in"
	OperatorGreaterEqual Operator = ">="
	OperatorLessEqual    Operator = "<="
	OperatorMod          Operator = "%"
	OperatorPlus         Operator = "+"
)
