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
)

func ExampleNew() {
	// case1 without DBOption
	db := New()
	fmt.Printf("case1 dialect: %s\n", db.dialect.name)

	// case2 use DBOption
	db = New(DBWithDialect(SQLite))
	fmt.Printf("case2 dialect: %s\n", db.dialect.name)

	// case3 share registry among DB
	registry := NewTagMetaRegistry()
	db1 := New(DBWithMetaRegistry(registry))
	db2 := New(DBWithMetaRegistry(registry))
	fmt.Printf("case3 same registry: %v", db1.metaRegistry == db2.metaRegistry)

	// Output:
	// case1 dialect: MySQL
	// case2 dialect: SQLite
	// case3 same registry: true
}

func ExampleDB_Delete() {
	db := New()
	tm := &TestModel{}
	query, _ := db.Delete().From(tm).Build()
	fmt.Printf("SQL: %s", query.SQL)
	// Output:
	// SQL: DELETE FROM `test_model`;
}

func ExampleDB_Insert() {
	db := New()
	tm := &TestModel{}
	query, _ := db.Insert().Values(tm).Build()
	fmt.Printf("SQL: %s", query.SQL)
	// Output:
	// SQL: INSERT INTO `test_model`(`id`,`first_name`,`age`,`last_name`) VALUES(?,?,?,?);
}

func ExampleDB_Select() {
	db := New()
	tm := &TestModel{}
	query, _ := db.Select().From(tm).Build()
	fmt.Printf("SQL: %s", query.SQL)
	// Output:
	// SQL: SELECT `id`,`first_name`,`age`,`last_name` FROM `test_model`;
}

func ExampleDB_Update() {
	db := New()
	tm := &TestModel{
		Age: 18,
	}
	query, _ := db.Update(tm).Build()
	fmt.Printf("SQL: %s", query.SQL)
	// Output:
	// SQL: UPDATE `test_model` SET `age`=?;
}
