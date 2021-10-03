// Copyright 2021 gotomicro
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package eql

type Column struct {
	name  string
	alias string
}

// C specify column
func C(c string) Column {
	return Column{
		name: c,
	}
}

// As means alias
func (c Column) As(alias string) Selectable {
	return Column{
		name: c.name,
		alias: alias,
	}
}

func (c Column) Add(val interface{}) MathExpr {
	return MathExpr{
		left: c,
		op: opAdd,
		right: valueOf(val),
	}
}

func (c Column) Multi(val interface{}) MathExpr {
	return MathExpr{
		left: c,
		op: opMulti,
		right: valueOf(val),
	}
}

func (Column) assign() {
	panic("implement me")
}

func (Column) expr() (string, error) {
	panic("implement me")
}

func (Column) selected() {
	panic("implement me")
}

type columns struct {
	cs []string
}

func (c columns) selected() {
	panic("implement me")
}

func (c columns) assign() {
	panic("implement me")
}

// Columns specify columns
func Columns(cs...string) columns {
	return columns{
		cs: cs,
	}
}

func valueOf(val interface{}) Expr {
	switch v := val.(type) {
	case Expr:
		return v
	default:
		return valueExpr{val: val}
	}
}

