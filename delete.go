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

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/gotomicro/eql/internal"
)

// Deleter builds DELETE query
type Deleter struct {
	SQL          string
	Args         []interface{}
	DeleteSqlMap map[string]string
}

// Build returns DELETE query
func (d *Deleter) Build() (*Query, error) {
	buildSql := d.DeleteSqlMap["scheme"]
	if _, ok := d.DeleteSqlMap["from"]; !ok {
		return &Query{}, fmt.Errorf(" no found from")
	}
	buildSql += d.DeleteSqlMap["from"]
	if _, ok := d.DeleteSqlMap["orderby"]; ok {
		buildSql += d.DeleteSqlMap["orderby"]
	}
	if _, ok := d.DeleteSqlMap["limit"]; ok {
		buildSql += d.DeleteSqlMap["limit"]
	}
	buildSql += ";"
	return &Query{SQL: buildSql, Args: d.Args}, nil
}

// From accepts model definition
func (d *Deleter) From(table interface{}) *Deleter {
	tableName, args := internal.TableName(table)
	sql := " FROM `" + tableName + "`"
	d.DeleteSqlMap["from"] = sql
	d.Args = args
	return &Deleter{Args: d.Args, DeleteSqlMap: d.DeleteSqlMap}
}

// Where accepts predicates
func (d *Deleter) Where(predicates ...Predicate) *Deleter {
	return &Deleter{}
}

// OrderBy means "ORDER BY"
func (d *Deleter) OrderBy(orderBy ...OrderBy) *Deleter {
	order_by := ""
	for _, val := range orderBy {
		for _, field := range val.fields {
			if val.asc {
				order_by += field + " ASC, "
			} else {
				order_by += field + " DESC, "
			}
		}
	}
	order_by = d.SQL + " ORDER by " + strings.Trim(order_by, ",")
	d.DeleteSqlMap["orderby"] = order_by
	return &Deleter{DeleteSqlMap: d.DeleteSqlMap}
}

// Limit limits the number of deleted rows
func (d *Deleter) Limit(limit int) *Deleter {
	sql := " LIMIT " + strconv.Itoa(limit)
	d.DeleteSqlMap["limit"] = sql
	return &Deleter{DeleteSqlMap: d.DeleteSqlMap}
}
