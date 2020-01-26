// Copyright 2019 The OctoSQL Authors.
// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package datatypes

import (
	"fmt"
	"regexp"
	"strings"

	"base/errors"

	"github.com/golang/protobuf/proto"
)

// octosql.AreEqual checks the equality of the given values, returning false if the types don't match.
func AreEqual(left, right Value) bool {
	return proto.Equal(&left, &right)
}

type Comparison int

const (
	LessThan    Comparison = -1
	Equal       Comparison = 0
	GreaterThan Comparison = 1
)

func Compare(x, y Value) (Comparison, error) {
	switch x.GetType() {
	case TypeInt:
		if y.GetType() != TypeInt {
			return 0, errors.Errorf("type mismatch between values")
		}

		x := x.AsInt()
		y := y.AsInt()

		if x == y {
			return 0, nil
		} else if x < y {
			return -1, nil
		}

		return 1, nil
	case TypeFloat:
		if y.GetType() != TypeFloat {
			return 0, errors.Errorf("type mismatch between values")
		}
		x := x.AsFloat()
		y := y.AsFloat()

		if x == y {
			return 0, nil
		} else if x < y {
			return -1, nil
		}

		return 1, nil
	case TypeString:
		if y.GetType() != TypeString {
			return 0, errors.Errorf("type mismatch between values")
		}

		x := x.AsString()
		y := y.AsString()

		if x == y {
			return 0, nil
		} else if x < y {
			return -1, nil
		}

		return 1, nil
	case TypeTime:
		if y.GetType() != TypeTime {
			return 0, errors.Errorf("type mismatch between values")
		}

		x := x.AsTime()
		y := y.AsTime()

		if x == y {
			return 0, nil
		} else if x.Before(y) {
			return -1, nil
		}

		return 1, nil
	case TypeBool:
		if y.GetType() != TypeBool {
			return 0, errors.Errorf("type mismatch between values")
		}

		x := x.AsBool()
		y := y.AsBool()

		if x == y {
			return 0, nil
		} else if !x && y {
			return -1, nil
		}

		return 1, nil

	case TypeNull, TypePhantom, TypeDuration, TypeTuple, TypeObject:
		return 0, errors.Errorf("unsupported type in sorting")
	}

	panic("unreachable")
}

var (
	re = regexp.MustCompile(`([^\\]?|[\\]{2})[%_]`)
)

func replacer(s string) string {
	if strings.HasPrefix(s, `\\`) {
		return s[2:]
	}

	result := strings.Replace(s, "%", ".*", -1)
	result = strings.Replace(result, "_", ".", -1)
	return result
}

func LikeToRegexp(likeExpr string) *regexp.Regexp {
	if likeExpr == "" {
		return regexp.MustCompile("^.*$") // Can never fail
	}

	keyPattern := regexp.QuoteMeta(likeExpr)
	keyPattern = re.ReplaceAllStringFunc(keyPattern, replacer)
	keyPattern = fmt.Sprintf("^%s$", keyPattern)
	return regexp.MustCompile(keyPattern) // Can never fail
}

func Like(likeExpr string, x Value) bool {
	re := LikeToRegexp(likeExpr)
	return re.Match([]byte(x.ToRawValue().(string)))
}
