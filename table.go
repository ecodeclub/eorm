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
		alias: "",
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

type Subquery struct {
	entity  TableReference
	q       QueryBuilder
	alias   string
	columns []Selectable
}

func (s Subquery) expr() (string, error) {
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
