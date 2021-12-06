package eql

import "fmt"

func ExampleNew() {
	// case1 without DBOption
	db := New()
	fmt.Printf("case1 dialect: %s\n", db.dialect.name)

	// case2 use DBOption
	db = New(DBWithDialect(SQLite))
	fmt.Printf("case2 dialect: %s", db.dialect.name)
	// Output:
	// case1 dialect: MySQL
	// case2 dialect: SQLite
}

func ExampleDB_Delete() {
	db := New()
	tm := &TestModel{}
	query, _ := db.Delete().From(tm).Build()
	fmt.Printf("SQL: %s", query.SQL)
	// Output:
	// DELETE FROM `test_model`
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
