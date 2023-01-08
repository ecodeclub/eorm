package eorm

type TableReference interface {
	tableAlias() string
}

type Table struct {
	entity any
	alias  string
}

func TableOf(entity any) Table {
	return Table{
		entity: entity,
	}
}

func (t Table) C(name string) Column {
	return Column{
		name:  name,
		table: t,
	}
}

func (t Table) tableAlias() string {
	return t.alias
}

func (t Table) As(alias string) Table {
	return Table{
		entity: t.entity,
		alias:  alias,
	}
}

func (t Table) Join(target TableReference) *JoinBuilder {
	return &JoinBuilder{
		left:  t,
		right: target,
		typ:   "JOIN",
	}
}

func (t Table) LeftJoin(target TableReference) *JoinBuilder {
	return &JoinBuilder{
		left:  t,
		right: target,
		typ:   "LEFT JOIN",
	}
}

func (t Table) RightJoin(target TableReference) *JoinBuilder {
	return &JoinBuilder{
		left:  t,
		right: target,
		typ:   "RIGHT JOIN",
	}
}

type Subquery struct {
	entity  TableReference
	q       QueryBuilder
	alias   string
	columns []Selectable
}

var _ TableReference = Subquery{}

func (Subquery) expr() (string, error) {
	panic("implement me")
}
func (s Subquery) tableAlias() string {
	return s.alias
}

var _ TableReference = Subquery{}

func (s Subquery) C(name string) Column {
	return Column{
		table: s,
		name:  name,
	}
}

func (s Subquery) Join(target TableReference) *JoinBuilder {
	return &JoinBuilder{
		left:  s,
		right: target,
		typ:   "JOIN",
	}
}

func (s Subquery) LeftJoin(target TableReference) *JoinBuilder {
	return &JoinBuilder{
		left:  s,
		right: target,
		typ:   "LEFT JOIN",
	}
}

func (s Subquery) RightJoin(target TableReference) *JoinBuilder {
	return &JoinBuilder{
		left:  s,
		right: target,
		typ:   "RIGHT JOIN",
	}
}

type JoinBuilder struct {
	left  TableReference
	right TableReference
	typ   string
}

type Join struct {
	left  TableReference
	right TableReference
	typ   string
	on    []Predicate
	using []string
}

var _ TableReference = Join{}

func (Join) tableAlias() string {
	return ""
}

func (j Join) Join(target TableReference) *JoinBuilder {
	return &JoinBuilder{
		left:  j,
		right: target,
		typ:   "JOIN",
	}
}
func (j Join) LeftJoin(target TableReference) *JoinBuilder {
	return &JoinBuilder{
		left:  j,
		right: target,
		typ:   "LEFT JOIN",
	}
}
func (j Join) RightJoin(target TableReference) *JoinBuilder {
	return &JoinBuilder{
		left:  j,
		right: target,
		typ:   "RIGHT JOIN",
	}
}

func (j *JoinBuilder) On(ps ...Predicate) Join {
	return Join{
		left:  j.left,
		right: j.right,
		typ:   j.typ,
		on:    ps,
	}
}

func (j *JoinBuilder) Using(s ...string) Join {
	return Join{
		left:  j.left,
		right: j.right,
		typ:   j.typ,
		using: s,
	}
}
