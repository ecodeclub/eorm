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
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/gotomicro/eorm/internal/dialect"
	"github.com/gotomicro/eorm/internal/model"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestDB_BeginTx(t *testing.T) {
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = mockDB.Close() }()

	db, err := openDB("mysql", mockDB)
	if err != nil {
		t.Fatal(err)
	}
	// Begin 失败
	mock.ExpectBegin().WillReturnError(errors.New("begin failed"))
	tx, err := db.BeginTx(context.Background(), &sql.TxOptions{})
	assert.Equal(t, errors.New("begin failed"), err)
	assert.Nil(t, tx)

	mock.ExpectBegin()
	tx, err = db.BeginTx(context.Background(), &sql.TxOptions{})
	assert.Nil(t, err)
	assert.NotNil(t, tx)
}

func TestDB_Wait(t *testing.T) {
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = mockDB.Close() }()

	db, err := openDB("mysql", mockDB)
	if err != nil {
		t.Fatal(err)
	}
	mock.ExpectPing()
	err = db.Wait()
	assert.Nil(t, err)
}

func ExampleDB_BeginTx() {
	db, _ := Open("sqlite3", "file:test.db?cache=shared&mode=memory")
	defer func() {
		_ = db.Close()
	}()
	tx, err := db.BeginTx(context.Background(), &sql.TxOptions{})
	if err == nil {
		fmt.Println("Begin")
	}
	// 或者 tx.Rollback()
	err = tx.Commit()
	if err == nil {
		fmt.Println("Commit")
	}
	// Output:
	// Begin
	// Commit
}

func ExampleOpen() {
	// case1 without DBOption
	db, _ := Open("sqlite3", "file:test.db?cache=shared&mode=memory")
	fmt.Printf("case1 dialect: %s\n", db.dialect.Name)

	// case2 use DBOption
	db, _ = Open("sqlite3", "file:test.db?cache=shared&mode=memory", WithDialect(dialect.MySQL))
	fmt.Printf("case2 dialect: %s\n", db.dialect.Name)

	// case3 share registry among DB
	registry := model.NewTagMetaRegistry()
	db1, _ := Open("sqlite3", "file:test.db?cache=shared&mode=memory", WithMetaRegistry(registry))
	db2, _ := Open("sqlite3", "file:test.db?cache=shared&mode=memory", WithMetaRegistry(registry))
	fmt.Printf("case3 same registry: %v", db1.metaRegistry == db2.metaRegistry)

	// Output:
	// case1 dialect: SQLite
	// case2 dialect: MySQL
	// case3 same registry: true
}

func ExampleDB_Delete() {
	db := memoryDB()
	tm := &TestModel{}
	query, _ := db.Delete().From(tm).Build()
	fmt.Printf("SQL: %s", query.SQL)
	// Output:
	// SQL: DELETE FROM `test_model`;
}

func ExampleDB_Update() {
	db := memoryDB()
	tm := &TestModel{
		Age: 18,
	}
	query, _ := db.Update(tm).Build()
	fmt.Printf("SQL: %s", query.SQL)
	// Output:
	// SQL: UPDATE `test_model` SET `id`=?,`first_name`=?,`age`=?,`last_name`=?;
}

// memoryDB 返回一个基于内存的 ORM，它使用的是 sqlite3 内存模式。
func memoryDB() *DB {
	orm, err := Open("sqlite3", "file:test.db?cache=shared&mode=memory")
	if err != nil {
		panic(err)
	}
	return orm
}

func memoryDBWithDB(db string) *DB {
	orm, err := Open("sqlite3", fmt.Sprintf("file:%s.db?cache=shared&mode=memory", db))
	if err != nil {
		panic(err)
	}
	return orm
}

