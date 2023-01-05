package eorm

type TableReference interface {
}

// Table 普通表
type Table struct {
	entity any
	alias  string
}

func TableOf(entity any) Table {
	return Table{
		entity: entity,
	}
}

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

func (t Table) As(alias string) Table {
	return Table{
		entity: t.entity,
		alias:  alias,
	}
}

func (t Table) C(name string) Column {
	return Column{
		name:  name,
		table: t,
	}
}
func (t Table) selected() {
	panic("implement me")
}

// Max represents MAX
func (t Table) Max(c string) Aggregate {
	return Aggregate{
		fn:    "MAX",
		arg:   c,
		table: t,
	}
}

// Max represents MAX
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

type Join struct {
	left  TableReference
	right TableReference
	on    []Predicate
	using []string
	typ   string
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
