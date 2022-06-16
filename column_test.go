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

import "fmt"

func ExampleC() {
	db := memoryDB()
	tm := &TestModel{}
	query, _ := NewSelector[TestModel](db).Select(C("Id")).From(tm).Where(C("Id").EQ(18)).Build()
	fmt.Printf(`
SQL: %s
Args: %v
`, query.SQL, query.Args)
	// Output:
	// SQL: SELECT `id` FROM `test_model` WHERE `id`=?;
	// Args: [18]
}

func ExampleColumn_EQ() {
	db := memoryDB()
	tm := &TestModel{}
	query, _ := NewSelector[TestModel](db).Select(C("Id")).From(tm).Where(C("Id").EQ(18)).Build()
	fmt.Printf(`
SQL: %s
Args: %v
`, query.SQL, query.Args)
	// Output:
	// SQL: SELECT `id` FROM `test_model` WHERE `id`=?;
	// Args: [18]
}

func ExampleColumn_Add() {
	db := memoryDB()
	tm := &TestModel{}
	query, _ := db.Update(tm).Set(Assign("Age", C("Age").Add(1))).Build()
	fmt.Printf(`
SQL: %s
Args: %v
`, query.SQL, query.Args)
	// Output:
	// SQL: UPDATE `test_model` SET `age`=(`age`+?);
	// Args: [1]
}

func ExampleColumn_As() {
	db := memoryDB()
	tm := &TestModel{}
	query, _ := NewSelector[TestModel](db).Select(C("Id").As("my_id")).From(tm).Build()
	fmt.Printf(`
SQL: %s
Args: %v
`, query.SQL, query.Args)
	// Output:
	// SQL: SELECT `id` AS `my_id` FROM `test_model`;
	// Args: []
}

func ExampleColumn_GT() {
	db := memoryDB()
	tm := &TestModel{}
	query, _ := NewSelector[TestModel](db).Select(C("Id")).From(tm).Where(C("Id").GT(18)).Build()
	fmt.Printf(`
SQL: %s
Args: %v
`, query.SQL, query.Args)
	// Output:
	// SQL: SELECT `id` FROM `test_model` WHERE `id`>?;
	// Args: [18]
}

func ExampleColumn_GTEQ() {
	db := memoryDB()
	tm := &TestModel{}
	query, _ := NewSelector[TestModel](db).Select(C("Id")).From(tm).Where(C("Id").GTEQ(18)).Build()
	fmt.Printf(`
SQL: %s
Args: %v
`, query.SQL, query.Args)
	// Output:
	// SQL: SELECT `id` FROM `test_model` WHERE `id`>=?;
	// Args: [18]
}

func ExampleColumn_LT() {
	db := memoryDB()
	tm := &TestModel{}
	query, _ := NewSelector[TestModel](db).Select(C("Id")).From(tm).Where(C("Id").LT(18)).Build()
	fmt.Printf(`
SQL: %s
Args: %v
`, query.SQL, query.Args)
	// Output:
	// SQL: SELECT `id` FROM `test_model` WHERE `id`<?;
	// Args: [18]
}

func ExampleColumn_LTEQ() {
	db := memoryDB()
	tm := &TestModel{}
	query, _ := NewSelector[TestModel](db).Select(C("Id")).From(tm).Where(C("Id").LTEQ(18)).Build()
	fmt.Printf(`
SQL: %s
Args: %v
`, query.SQL, query.Args)
	// Output:
	// SQL: SELECT `id` FROM `test_model` WHERE `id`<=?;
	// Args: [18]
}

func ExampleColumn_Multi() {
	db := memoryDB()
	tm := &TestModel{}
	query, _ := db.Update(tm).Set(Assign("Age", C("Age").Multi(2))).Build()
	fmt.Printf(`
SQL: %s
Args: %v
`, query.SQL, query.Args)
	// Output:
	// SQL: UPDATE `test_model` SET `age`=(`age`*?);
	// Args: [2]
}

func ExampleColumn_NEQ() {
	db := memoryDB()
	tm := &TestModel{}
	query, _ := NewSelector[TestModel](db).Select(C("Id")).From(tm).Where(C("Id").NEQ(18)).Build()
	fmt.Printf(`
SQL: %s
Args: %v
`, query.SQL, query.Args)
	// Output:
	// SQL: SELECT `id` FROM `test_model` WHERE `id`!=?;
	// Args: [18]
}

func ExampleColumns() {
	db := memoryDB()
	tm := &TestModel{}
	query, _ := NewSelector[TestModel](db).Select(Columns("Id", "Age")).From(tm).Build()
	fmt.Printf(`
SQL: %s
Args: %v
`, query.SQL, query.Args)
	// Output:
	// SQL: SELECT `id`,`age` FROM `test_model`;
	// Args: []
}
