// Copyright 2021 ecodeclub
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
	"testing"

	"github.com/ecodeclub/eorm/internal/datasource/single"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/ecodeclub/eorm/internal/errs"
	"github.com/stretchr/testify/require"

	"github.com/stretchr/testify/assert"
)

func TestUpdater_Set(t *testing.T) {
	tm := &TestModel{
		Id:        12,
		FirstName: "Tom",
		Age:       18,
		LastName:  &sql.NullString{String: "Jerry", Valid: true},
	}
	db := memoryDB()
	testCases := []CommonTestCase{
		{
			name:     "no set and update",
			builder:  NewUpdater[TestModel](db),
			wantSql:  "UPDATE `test_model` SET `id`=?,`first_name`=?,`age`=?,`last_name`=?;",
			wantArgs: []interface{}{int64(0), "", int8(0), (*sql.NullString)(nil)},
		},
		{
			name: "no set",
			builder: NewUpdater[TestModel](db).Update(&TestModel{
				Id:        12,
				FirstName: "Tom",
				Age:       18,
			}),
			wantSql:  "UPDATE `test_model` SET `id`=?,`first_name`=?,`age`=?,`last_name`=?;",
			wantArgs: []interface{}{int64(12), "Tom", int8(18), (*sql.NullString)(nil)},
		},
		{
			name:     "set columns",
			builder:  NewUpdater[TestModel](db).Update(tm).Set(Columns("FirstName", "Age")),
			wantSql:  "UPDATE `test_model` SET `first_name`=?,`age`=?;",
			wantArgs: []interface{}{"Tom", int8(18)},
		},
		{
			name:    "set invalid columns",
			builder: NewUpdater[TestModel](db).Update(tm).Set(Columns("FirstNameInvalid", "Age")),
			wantErr: errs.NewInvalidFieldError("FirstNameInvalid"),
		},
		{
			name:     "set c2",
			builder:  NewUpdater[TestModel](db).Update(tm).Set(C("FirstName"), C("Age")),
			wantSql:  "UPDATE `test_model` SET `first_name`=?,`age`=?;",
			wantArgs: []interface{}{"Tom", int8(18)},
		},
		{
			name:    "set invalid c2",
			builder: NewUpdater[TestModel](db).Update(tm).Set(C("FirstNameInvalid"), C("Age")),
			wantErr: errs.NewInvalidFieldError("FirstNameInvalid"),
		},
		{
			name:     "set assignment",
			builder:  NewUpdater[TestModel](db).Update(tm).Set(C("FirstName"), Assign("Age", 30)),
			wantSql:  "UPDATE `test_model` SET `first_name`=?,`age`=?;",
			wantArgs: []interface{}{"Tom", 30},
		},
		{
			name:    "set invalid assignment",
			builder: NewUpdater[TestModel](db).Update(tm).Set(C("FirstName"), Assign("InvalidAge", 30)),
			wantErr: errs.NewInvalidFieldError("InvalidAge"),
		},
		{
			name:     "set age+1",
			builder:  NewUpdater[TestModel](db).Update(tm).Set(C("FirstName"), Assign("Age", C("Age").Add(1))),
			wantSql:  "UPDATE `test_model` SET `first_name`=?,`age`=(`age`+?);",
			wantArgs: []interface{}{"Tom", 1},
		},
		{
			name:     "set age=id+1",
			builder:  NewUpdater[TestModel](db).Update(tm).Set(C("FirstName"), Assign("Age", C("Id").Add(10))),
			wantSql:  "UPDATE `test_model` SET `first_name`=?,`age`=(`id`+?);",
			wantArgs: []interface{}{"Tom", 10},
		},
		{
			name:     "set age=id+(age*100)+10",
			builder:  NewUpdater[TestModel](db).Update(tm).Set(C("FirstName"), Assign("Age", C("Id").Add(C("Age").Multi(100)).Add(10))),
			wantSql:  "UPDATE `test_model` SET `first_name`=?,`age`=((`id`+(`age`*?))+?);",
			wantArgs: []interface{}{"Tom", 100, 10},
		},
		{
			name:     "set age=(id+(age*100))*110",
			builder:  NewUpdater[TestModel](db).Update(tm).Set(C("FirstName"), Assign("Age", C("Id").Add(C("Age").Multi(100)).Multi(110))),
			wantSql:  "UPDATE `test_model` SET `first_name`=?,`age`=((`id`+(`age`*?))*?);",
			wantArgs: []interface{}{"Tom", 100, 110},
		},
		{
			name:     "not nil columns",
			builder:  NewUpdater[TestModel](db).Update(&TestModel{Id: 13}).SkipNilValue(),
			wantSql:  "UPDATE `test_model` SET `id`=?,`first_name`=?,`age`=?;",
			wantArgs: []interface{}{int64(13), "", int8(0)},
		},
		{
			name:     "not zero columns",
			builder:  NewUpdater[TestModel](db).Update(&TestModel{Id: 13}).SkipZeroValue(),
			wantSql:  "UPDATE `test_model` SET `id`=?;",
			wantArgs: []interface{}{int64(13)},
		},
	}

	for _, tc := range testCases {
		c := tc
		t.Run(c.name, func(t *testing.T) {
			query, err := tc.builder.Build()
			assert.Equal(t, err, c.wantErr)
			if err != nil {
				return
			}
			assert.Equal(t, c.wantSql, query.SQL)
			assert.Equal(t, c.wantArgs, query.Args)
		})
	}
}

func TestUpdater_SetForCombination(t *testing.T) {
	u := &User{
		Id: 12,
		Person: Person{
			FirstName: "Tom",
			Age:       18,
			LastName:  &sql.NullString{String: "Jerry", Valid: true},
		},
	}
	db := memoryDB()
	testCases := []CommonTestCase{
		{
			name:     "no set",
			builder:  NewUpdater[User](db).Update(u),
			wantSql:  "UPDATE `user` SET `id`=?,`first_name`=?,`age`=?,`last_name`=?;",
			wantArgs: []interface{}{int64(12), "Tom", int8(18), &sql.NullString{String: "Jerry", Valid: true}},
		},
		{
			name:     "set columns",
			builder:  NewUpdater[User](db).Update(u).Set(Columns("FirstName", "Age")),
			wantSql:  "UPDATE `user` SET `first_name`=?,`age`=?;",
			wantArgs: []interface{}{"Tom", int8(18)},
		},
		{
			name:    "set invalid columns",
			builder: NewUpdater[User](db).Update(u).Set(Columns("FirstNameInvalid", "Age")),
			wantErr: errs.NewInvalidFieldError("FirstNameInvalid"),
		},
		{
			name:     "set c2",
			builder:  NewUpdater[User](db).Update(u).Set(C("FirstName"), C("Age")),
			wantSql:  "UPDATE `user` SET `first_name`=?,`age`=?;",
			wantArgs: []interface{}{"Tom", int8(18)},
		},

		{
			name:    "set invalid c2",
			builder: NewUpdater[User](db).Update(u).Set(C("FirstNameInvalid"), C("Age")),
			wantErr: errs.NewInvalidFieldError("FirstNameInvalid"),
		},

		{
			name:     "set assignment",
			builder:  NewUpdater[User](db).Update(u).Set(C("FirstName"), Assign("Age", 30)),
			wantSql:  "UPDATE `user` SET `first_name`=?,`age`=?;",
			wantArgs: []interface{}{"Tom", 30},
		},
		{
			name:    "set invalid assignment",
			builder: NewUpdater[User](db).Update(u).Set(C("FirstName"), Assign("InvalidAge", 30)),
			wantErr: errs.NewInvalidFieldError("InvalidAge"),
		},
		{
			name:     "set age+1",
			builder:  NewUpdater[User](db).Update(u).Set(C("FirstName"), Assign("Age", C("Age").Add(1))),
			wantSql:  "UPDATE `user` SET `first_name`=?,`age`=(`age`+?);",
			wantArgs: []interface{}{"Tom", 1},
		},
		{
			name:     "set age=id+1",
			builder:  NewUpdater[User](db).Update(u).Set(C("FirstName"), Assign("Age", C("Id").Add(10))),
			wantSql:  "UPDATE `user` SET `first_name`=?,`age`=(`id`+?);",
			wantArgs: []interface{}{"Tom", 10},
		},
		{
			name:     "set age=id+(age*100)",
			builder:  NewUpdater[User](db).Update(u).Set(C("FirstName"), Assign("Age", C("Id").Add(C("Age").Multi(100)))),
			wantSql:  "UPDATE `user` SET `first_name`=?,`age`=(`id`+(`age`*?));",
			wantArgs: []interface{}{"Tom", 100},
		},
		{
			name:     "set age=(id+(age*100))*110",
			builder:  NewUpdater[User](db).Update(u).Set(C("FirstName"), Assign("Age", C("Id").Add(C("Age").Multi(100)).Multi(110))),
			wantSql:  "UPDATE `user` SET `first_name`=?,`age`=((`id`+(`age`*?))*?);",
			wantArgs: []interface{}{"Tom", 100, 110},
		},
		{
			name:     "not nil columns",
			builder:  NewUpdater[User](db).Update(&User{Id: 13}).SkipNilValue(),
			wantSql:  "UPDATE `user` SET `id`=?,`first_name`=?,`age`=?;",
			wantArgs: []interface{}{int64(13), "", int8(0)},
		},
		{
			name:     "not zero columns",
			builder:  NewUpdater[User](db).Update(&User{Id: 13}).SkipZeroValue(),
			wantSql:  "UPDATE `user` SET `id`=?;",
			wantArgs: []interface{}{int64(13)},
		},
	}

	for _, tc := range testCases {
		c := tc
		t.Run(c.name, func(t *testing.T) {
			query, err := tc.builder.Build()
			assert.Equal(t, err, c.wantErr)
			if err != nil {
				return
			}
			assert.Equal(t, c.wantSql, query.SQL)
			assert.Equal(t, c.wantArgs, query.Args)
		})
	}
}

func TestUpdater_Exec(t *testing.T) {
	tm := &TestModel{
		Id:        12,
		FirstName: "Tom",
		Age:       18,
		LastName:  &sql.NullString{String: "Jerry", Valid: true},
	}
	testCases := []struct {
		name      string
		u         *Updater[TestModel]
		update    func(*DB, *testing.T) Result
		wantErr   error
		mockOrder func(mock sqlmock.Sqlmock)
		wantVal   sql.Result
	}{
		{
			name: "update err",
			update: func(db *DB, t *testing.T) Result {
				updater := NewUpdater[TestModel](db).Set(Assign("Age", 12))
				result := updater.Exec(context.Background())
				return result
			},
			mockOrder: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("UPDATE `test_model` SET `age`=").
					WithArgs(int64(12)).
					WillReturnError(errors.New("no such table: test_model"))
			},
			wantErr: errors.New("no such table: test_model"),
		},
		{
			name: "specify columns",
			update: func(db *DB, t *testing.T) Result {
				updater := NewUpdater[TestModel](db).Update(tm).Set(Columns("FirstName"))
				result := updater.Exec(context.Background())
				return result
			},
			mockOrder: func(mock sqlmock.Sqlmock) {
				mock.ExpectExec("UPDATE `test_model` SET `first_name`=").
					WithArgs("Tom").WillReturnResult(sqlmock.NewResult(100, 1))
			},
			wantVal: sqlmock.NewResult(100, 1),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mockDB, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			db, err := OpenDS("mysql", single.NewDB(mockDB))
			defer func(db *DB) { _ = db.Close() }(db)
			if err != nil {
				t.Fatal(err)
			}
			tc.mockOrder(mock)

			res := tc.update(db, t)
			if res.Err() != nil {
				assert.Equal(t, tc.wantErr, res.Err())
				return
			}
			assert.Nil(t, tc.wantErr)
			rowsAffectedExpect, err := tc.wantVal.RowsAffected()
			require.NoError(t, err)
			rowsAffected, err := res.RowsAffected()
			require.NoError(t, err)

			lastInsertIdExpected, err := tc.wantVal.LastInsertId()
			require.NoError(t, err)
			lastInsertId, err := res.LastInsertId()
			require.NoError(t, err)
			assert.Equal(t, lastInsertIdExpected, lastInsertId)
			assert.Equal(t, rowsAffectedExpect, rowsAffected)

			if err = mock.ExpectationsWereMet(); err != nil {
				t.Error(err)
			}
		})
	}
}

func ExampleUpdater_SkipNilValue() {
	db := memoryDB()
	query, _ := NewUpdater[TestModel](db).Update(&TestModel{Id: 13}).SkipNilValue().Build()
	fmt.Println(query.String())
	// Output:
	// SQL: UPDATE `test_model` SET `id`=?,`first_name`=?,`age`=?;
	// Args: []interface {}{13, "", 0}
}

func ExampleUpdater_SkipZeroValue() {
	db := memoryDB()
	query, _ := NewUpdater[TestModel](db).Update(&TestModel{Id: 13}).SkipZeroValue().Build()
	fmt.Println(query.String())
	// Output:
	// SQL: UPDATE `test_model` SET `id`=?;
	// Args: []interface {}{13}
}

type Person struct {
	FirstName string
	Age       int8
	LastName  *sql.NullString
}
type User struct {
	Id int64 `eorm:"auto_increment,primary_key"`
	Person
}
