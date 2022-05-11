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

import (
	"github.com/valyala/bytebufferpool"
)

// DBOption configure DB
type DBOption func(db *DB)

// DB represents a database
type DB struct {
	metaRegistry MetaRegistry
	dialect      Dialect
}

// NewDB returns DB.
// By default, it will create an instance of MetaRegistry and use MySQL as the dialect
func NewDB(opts ...DBOption) *DB {
	db := &DB{
		metaRegistry: &tagMetaRegistry{},
		dialect:      MySQL,
	}
	for _, o := range opts {
		o(db)
	}
	return db
}

// DBWithMetaRegistry specify the MetaRegistry
func DBWithMetaRegistry(registry MetaRegistry) DBOption {
	return func(db *DB) {
		db.metaRegistry = registry
	}
}

// DBWithDialect specify dialect
func DBWithDialect(dialect Dialect) DBOption {
	return func(db *DB) {
		db.dialect = dialect
	}
}

// Delete starts a "delete" query.
func (db *DB) Delete() *Deleter {
	return &Deleter{
		builder: db.builder(),
	}
}

func (db *DB) Update(table interface{}) *Updater {
	return &Updater{
		builder: db.builder(),
		table:   table,
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
