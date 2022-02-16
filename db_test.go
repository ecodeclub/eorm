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
	db := NewDB()
	fmt.Printf("case1 dialect: %s\n", db.dialect.name)

	// case2 use DBOption
	db = NewDB(DBWithDialect(SQLite))
	fmt.Printf("case2 dialect: %s\n", db.dialect.name)

	// case3 share registry among DB
	registry := NewTagMetaRegistry()
	db1 := NewDB(DBWithMetaRegistry(registry))
	db2 := NewDB(DBWithMetaRegistry(registry))
	fmt.Printf("case3 same registry: %v", db1.metaRegistry == db2.metaRegistry)

	// Output:
	// case1 dialect: MySQL
	// case2 dialect: SQLite
	// case3 same registry: true
}

func ExampleDB_Delete() {
	db := NewDB()
	tm := &TestModel{}
	query, _ := db.Delete().From(tm).Build()
	fmt.Printf("SQL: %s", query.SQL)
	// Output:
	// SQL: DELETE FROM `test_model`;
}

func ExampleDB_Insert() {
	db := NewDB()
	tm := &TestModel{}
	query, _ := db.Insert().Values(tm).Build()
	fmt.Printf("SQL: %s", query.SQL)
	// Output:
	// SQL: INSERT INTO `test_model`(`id`,`first_name`,`age`,`last_name`) VALUES(?,?,?,?);
}

func ExampleSelector_Select() {
	db := NewDB()
	tm := &TestModel{}
	cases := []*Selector{
		// case0: all columns are included
		NewSelector(db).From(tm),
		// case1: only query specific columns
		NewSelector(db).Select(Columns("Id", "Age")).From(tm),
		// case2: using alias
		NewSelector(db).Select(C("Id").As("my_id")).From(tm),
		// case3: using aggregation function and alias
		NewSelector(db).Select(Avg("Age").As("avg_age")).From(tm),
		// case4: using raw expression
		NewSelector(db).Select(Raw("COUNT(DISTINCT `age`) AS `age_cnt`")).From(tm),
	}

	for index, tc := range cases {
		query, _ := tc.Build()
		fmt.Printf("case%d:\n%s", index, query.string())
	}
	// Output:
	// case0:
	// SQL: SELECT `id`,`first_name`,`age`,`last_name` FROM `test_model`;
	// Args: []interface {}(nil)
	// case1:
	// SQL: SELECT `id`,`age` FROM `test_model`;
	// Args: []interface {}(nil)
	// case2:
	// SQL: SELECT `id` AS `my_id` FROM `test_model`;
	// Args: []interface {}(nil)
	// case3:
	// SQL: SELECT AVG(`age`) AS `avg_age` FROM `test_model`;
	// Args: []interface {}(nil)
	// case4:
	// SQL: SELECT COUNT(DISTINCT `age`) AS `age_cnt` FROM `test_model`;
	// Args: []interface {}(nil)
}

func ExampleDB_Update() {
	db := NewDB()
	tm := &TestModel{
		Age: 18,
	}
	query, _ := db.Update(tm).Build()
	fmt.Printf("SQL: %s", query.SQL)
	// Output:
	// SQL: UPDATE `test_model` SET `age`=?;
}
