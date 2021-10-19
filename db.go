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
	"reflect"

	"github.com/valyala/bytebufferpool"
)

// DBOption configure DB
type DBOption func(db *DB)

// DB represents a database
type DB struct {
	metaRegistry   MetaRegistry
	dialect        Dialect
	nullAssertFunc NullAssertFunc
}

// New returns DB. It's the entry of EQL
func New(opts ...DBOption) *DB {
	db := &DB{
		metaRegistry:   defaultMetaRegistry,
		dialect:        mysql,
		nullAssertFunc: NilAsNullFunc,
	}
	for _, o := range opts {
		o(db)
	}
	return db
}

// Select starts a select query. If columns are empty, all columns will be fetched
func (db *DB) Select(columns ...Selectable) *Selector {
	return &Selector{
		builder: db.builder(),
		columns: columns,
	}
}

// Delete starts a "delete" query.
func (*DB) Delete() *Deleter {
	panic("implement me")
}

func (db *DB) Update(table interface{}) *Updater {
	return &Updater{
		builder:        db.builder(),
		table:          table,
		nullAssertFunc: db.nullAssertFunc,
	}
}

// Insert generate Inserter to builder insert query
func (db *DB) Insert() *Inserter {
	return &Inserter{
		builder: db.builder(),
	}
}

func (db *DB) builder() builder {
	return builder{
		registry: db.metaRegistry,
		dialect:  db.dialect,
		buffer:   bytebufferpool.Get(),
	}
}

func WithNullAssertFunc(nullable NullAssertFunc) DBOption {
	return func(db *DB) {
		db.nullAssertFunc = nullable
	}
}

// NullAssertFunc determined if the value is NULL.
// As we know, there is a gap between NULL and nil
// There are two kinds of nullAssertFunc
// 1. nil = NULL, see NilAsNullFunc
// 2. zero value = NULL, see ZeroAsNullFunc
type NullAssertFunc func(val interface{}) bool

// NilAsNullFunc use the strict definition of "nullAssertFunc"
// if and only if the val is nil, indicates value is null
func NilAsNullFunc(val interface{}) bool {
	return val == nil
}

// ZeroAsNullFunc means "zero value = null"
func ZeroAsNullFunc(val interface{}) bool {
	if val == nil {
		return true
	}
	switch v := val.(type) {
	case int:
		return v == 0
	case int8:
		return v == 0
	case int16:
		return v == 0
	case int32:
		return v == 0
	case int64:
		return v == 0
	case uint:
		return v == 0
	case uint8:
		return v == 0
	case uint16:
		return v == 0
	case uint32:
		return v == 0
	case uint64:
		return v == 0
	case float32:
		return v == 0
	case float64:
		return v == 0
	case bool:
		return v
	case string:
		return v == ""
	default:
		valRef := reflect.ValueOf(val)
		return valRef.IsZero()
	}
}
