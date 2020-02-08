// Copyright 2019 The OctoSQL Authors.
// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package expressions

import (
	"fmt"

	"datavalues"

	"base/errors"
)

type IValidator interface {
	Validate(args ...*datavalues.Value) error
}

type ISingleArgumentValidator interface {
	Validate(arg *datavalues.Value) error
}

type all struct {
	validators []IValidator
}

func All(validators ...IValidator) *all {
	return &all{validators: validators}
}

func (v *all) Validate(args ...*datavalues.Value) error {
	for _, validator := range v.validators {
		err := validator.Validate(args...)
		if err != nil {
			return err
		}
	}
	return nil
}

type oneOf struct {
	validators []IValidator
}

func OneOf(validators ...IValidator) *oneOf {
	return &oneOf{validators: validators}
}

func (v *oneOf) Validate(args ...*datavalues.Value) error {
	errs := make([]error, len(v.validators))
	for i, validator := range v.validators {
		errs[i] = validator.Validate(args...)
		if errs[i] == nil {
			return nil
		}
	}
	return errors.Errorf("none of the conditions have been met: %+v", errs)
}

type singleOneOf struct {
	validators []ISingleArgumentValidator
}

func SingleOneOf(validators ...ISingleArgumentValidator) *singleOneOf {
	return &singleOneOf{validators: validators}
}

func (v *singleOneOf) Validate(arg *datavalues.Value) error {
	errs := make([]error, len(v.validators))
	for i, validator := range v.validators {
		errs[i] = validator.Validate(arg)
		if errs[i] == nil {
			return nil
		}
	}
	return fmt.Errorf("none of the conditions have been met: %+v", errs)
}

type exactlyNArgs struct {
	n int
}

func ExactlyNArgs(n int) *exactlyNArgs {
	return &exactlyNArgs{n: n}
}

func (v *exactlyNArgs) Validate(args ...*datavalues.Value) error {
	if len(args) != v.n {
		return errors.Errorf("expected exactly %s, but got %v", argumentCount(v.n), len(args))
	}
	return nil
}

type typeOf struct {
	wantedType *datavalues.Value
}

func TypeOf(wantedType *datavalues.Value) *typeOf {
	return &typeOf{wantedType: wantedType}
}

func (v *typeOf) Validate(arg *datavalues.Value) error {
	if v.wantedType.GetType() != arg.GetType() {
		return errors.Errorf("expected type %v but got %v", v.wantedType.GetType(), arg.GetType())
	}
	return nil
}

type ifArgPresent struct {
	i         int
	validator IValidator
}

func IfArgPresent(i int, validator IValidator) *ifArgPresent {
	return &ifArgPresent{i: i, validator: validator}
}

func (v *ifArgPresent) Validate(args ...*datavalues.Value) error {
	if len(args) < v.i+1 {
		return nil
	}
	return v.validator.Validate(args...)
}

type atLeastNArgs struct {
	n int
}

func AtLeastNArgs(n int) *atLeastNArgs {
	return &atLeastNArgs{n: n}
}

func (v *atLeastNArgs) Validate(args ...*datavalues.Value) error {
	if len(args) < v.n {
		return errors.Errorf("expected at least %s, but got %v", argumentCount(v.n), len(args))
	}
	return nil
}

type atMostNArgs struct {
	n int
}

func AtMostNArgs(n int) *atMostNArgs {
	return &atMostNArgs{n: n}
}

func (v *atMostNArgs) Validate(args ...*datavalues.Value) error {
	if len(args) > v.n {
		return errors.Errorf("expected at most %s, but got %v", argumentCount(v.n), len(args))
	}
	return nil
}

type arg struct {
	i         int
	validator ISingleArgumentValidator
}

func Arg(i int, validator ISingleArgumentValidator) *arg {
	return &arg{i: i, validator: validator}
}

func (v *arg) Validate(args ...*datavalues.Value) error {
	if err := v.validator.Validate(args[v.i]); err != nil {
		return fmt.Errorf("bad argument at index %v: %v", v.i, err)
	}
	return nil
}

type allArgs struct {
	validator ISingleArgumentValidator
}

func AllArgs(validator ISingleArgumentValidator) *allArgs {
	return &allArgs{validator: validator}
}

func (v *allArgs) Validate(args ...*datavalues.Value) error {
	for i := range args {
		if err := v.validator.Validate(args[i]); err != nil {
			return errors.Errorf("bad argument at index %v: %v", i, err)
		}
	}
	return nil
}

func argumentCount(n int) string {
	switch n {
	case 1:
		return "1 argument"
	default:
		return fmt.Sprintf("%d arguments", n)
	}
}
