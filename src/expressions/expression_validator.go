// Copyright 2019 The OctoSQL Authors.
// Copyright 2020 The VectorSQL Authors.
//
// Code is licensed under Apache License, Version 2.0.

package expressions

import (
	"fmt"

	"base/docs"
	"base/errors"
	"datavalues"
)

type IValidator interface {
	docs.Documented
	Validate(args ...*datavalues.Value) error
}

type ISingleArgumentValidator interface {
	docs.Documented
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

func (v *all) Document() docs.Documentation {
	childDocs := make([]docs.Documentation, len(v.validators))
	for i := range v.validators {
		childDocs[i] = v.validators[i].Document()
	}
	return docs.List(childDocs...)
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

func (v *oneOf) Document() docs.Documentation {
	childDocs := make([]docs.Documentation, len(v.validators))
	for i := range v.validators {
		childDocs[i] = v.validators[i].Document()
	}

	return docs.Paragraph(docs.Text("must satisfy one of"), docs.List(childDocs...))
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

func (v *singleOneOf) Document() docs.Documentation {
	childDocs := make([]docs.Documentation, len(v.validators))
	for i := range v.validators {
		childDocs[i] = v.validators[i].Document()
	}

	return docs.Paragraph(docs.Text("must satisfy one of the following"), docs.List(childDocs...))
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

func (v *exactlyNArgs) Document() docs.Documentation {
	return docs.Text(fmt.Sprintf("exactly %s must be provided", argumentCount(v.n)))
}

type typeOf struct {
	wantedType *datavalues.Value
}

func TypeOf(wantedType *datavalues.Value) *typeOf {
	return &typeOf{wantedType: wantedType}
}

func (v *typeOf) Validate(arg *datavalues.Value) error {
	if v.wantedType.GetType() != arg.GetType() {
		return errors.Errorf("expected type %v but got %v", v.wantedType.Document(), arg.Document())
	}
	return nil
}

func (v *typeOf) Document() docs.Documentation {
	return docs.Paragraph(docs.Text("must be of type"), v.wantedType.Document())
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

func (v *ifArgPresent) Document() docs.Documentation {
	return docs.Paragraph(
		docs.Text(fmt.Sprintf("if the %s argument is provided, then", docs.Ordinal(v.i+1))),
		v.validator.Document(),
	)
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

func (v *atLeastNArgs) Document() docs.Documentation {
	return docs.Text(fmt.Sprintf("at least %s may be provided", argumentCount(v.n)))
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

func (v *atMostNArgs) Document() docs.Documentation {
	return docs.Text(fmt.Sprintf("at most %s may be provided", argumentCount(v.n)))
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

func (v *arg) Document() docs.Documentation {
	return docs.Paragraph(
		docs.Text(fmt.Sprintf("the %s argument", docs.Ordinal(v.i+1))),
		v.validator.Document(),
	)
}

type sameType struct {
	idxs []int
}

func SameType(idx ...int) *sameType {
	return &sameType{idxs: idx}
}

func (v *sameType) Validate(args ...*datavalues.Value) error {
	var current *datavalues.Value
	for i, idx := range v.idxs {
		arg := args[idx]
		if current == nil {
			current = arg
		}
		if current.GetType() != arg.GetType() {
			return fmt.Errorf("bad argument type at index %v, wanted:%v, got:%v", i, current.Document(), arg.Document())
		}
	}
	return nil
}

func (v *sameType) Document() docs.Documentation {
	return docs.Text(fmt.Sprintf("index %+v type must be same", v.idxs))
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

func (v *allArgs) Document() docs.Documentation {
	return docs.Paragraph(
		docs.Text("all arguments"),
		v.validator.Document(),
	)
}

func argumentCount(n int) string {
	switch n {
	case 1:
		return "1 argument"
	default:
		return fmt.Sprintf("%d arguments", n)
	}
}
