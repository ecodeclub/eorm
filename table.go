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

package eorm

type TableReference interface {
	getAlias() string
}

// Table 普通表
type Table struct {
	entity any
	alias  string
}

// TableOf 创建一个 Table 代表一个表
// alias 是指该表的别名
// 例如 SELECT * FROM user_tab AS t1，t1 就是别名
func TableOf(entity any, alias string) Table {
	return Table{
		entity: entity,
		alias:  alias,
	}
}

func (t Table) getAlias() string {
	return t.alias
}

// Join 查询
func (t Table) Join(right TableReference) *JoinBuilder {
	return &JoinBuilder{
		left:  t,
		right: right,
		typ:   "JOIN",
	}
}

func (t Table) LeftJoin(right TableReference) *JoinBuilder {
	return &JoinBuilder{
		left:  t,
		right: right,
		typ:   "LEFT JOIN",
	}
}

func (t Table) RightJoin(right TableReference) *JoinBuilder {
	return &JoinBuilder{
		left:  t,
		right: right,
		typ:   "RIGHT JOIN",
	}
}

func (t Table) C(name string) Column {
	return Column{
		name:  name,
		table: t,
	}
}

// Max represents MAX
func (t Table) Max(c string) Aggregate {
	return Aggregate{
		fn:    "MAX",
		arg:   c,
		table: t,
	}
}

// Avg represents AVG
func (t Table) Avg(c string) Aggregate {
	return Aggregate{
		fn:    "AVG",
		arg:   c,
		table: t,
	}
}

// Min represents MIN
func (t Table) Min(c string) Aggregate {
	return Aggregate{
		fn:    "MIN",
		arg:   c,
		table: t,
	}
}

// Count represents COUNT
func (t Table) Count(c string) Aggregate {
	return Aggregate{
		fn:    "COUNT",
		arg:   c,
		table: t,
	}
}

// Sum represents SUM
func (t Table) Sum(c string) Aggregate {
	return Aggregate{
		fn:    "SUM",
		arg:   c,
		table: t,
	}
}

func (t Table) AllColumns() RawExpr {
	return Raw("`" + t.alias + "`.*")
}

type Join struct {
	left  TableReference
	right TableReference
	on    []Predicate
	using []string
	typ   string
}

func (Join) getAlias() string {
	return ""
}

func (j Join) Join(reference TableReference) *JoinBuilder {
	return &JoinBuilder{
		left:  j,
		right: reference,
		typ:   "JOIN",
	}
}

func (j Join) LeftJoin(reference TableReference) *JoinBuilder {
	return &JoinBuilder{
		left:  j,
		right: reference,
		typ:   "LEFT JOIN",
	}
}

func (j Join) RightJoin(reference TableReference) *JoinBuilder {
	return &JoinBuilder{
		left:  j,
		right: reference,
		typ:   "RIGHT JOIN",
	}
}

type JoinBuilder struct {
	left  TableReference
	right TableReference
	typ   string
}

func (j *JoinBuilder) On(ps ...Predicate) Join {
	return Join{
		left:  j.left,
		right: j.right,
		typ:   j.typ,
		on:    ps,
	}
}

func (j *JoinBuilder) Using(cols ...string) Join {
	return Join{
		left:  j.left,
		right: j.right,
		typ:   j.typ,
		using: cols,
	}
}
