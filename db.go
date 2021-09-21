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

// DBOption configure DB
type DBOption func(db *DB)

// DB represents a database
type DB struct {
	config           *config
	metaTableFactory MetaTableFactory
}

// New returns DB. It's the entry of EQL
func New(opts ...DBOption) *DB {
	db := &DB{
		config:           nil,
		metaTableFactory: &NewMetaTable{},
	}
	for _, o := range opts {
		o(db)
	}
	return db
}

// Select starts a select query. If columns are empty, all columns will be fetched
func (*DB) Select(columns ...Selectable) *Selector {
	panic("implement me")
}

// Delete starts a "delete" query.
func (*DB) Delete() *Deleter {
	return &Deleter{}
}
