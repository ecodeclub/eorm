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
	"fmt"
	"testing"

	"github.com/ecodeclub/eorm/internal/datasource/single"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/ecodeclub/eorm/internal/errs"
	"github.com/ecodeclub/eorm/internal/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRawQuery_Get_baseType(t *testing.T) {
	mockDB, mock, err := sqlmock.New(
		sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = mockDB.Close() }()
	db, err := OpenDS("mysql", single.NewDB(mockDB))
	if err != nil {
		t.Fatal(err)
	}

	testCases := []struct {
		name      string
		queryRes  func(t *testing.T) any
		mockErr   error
		mockOrder func(mock sqlmock.Sqlmock)
		wantErr   error
		wantVal   any
	}{
		{
			name: "res RawQuery int",
			queryRes: func(t *testing.T) any {
				queryer := RawQuery[int](db, "SELECT `age` FROM `test_model` AS `t1` LIMIT ?;", 1)
				result, err := queryer.Get(context.Background())
				require.NoError(t, err)
				return result
			},
			mockOrder: func(mock sqlmock.Sqlmock) {
				rows := mock.NewRows([]string{"age"}).AddRow(10)
				mock.ExpectQuery("SELECT `age` FROM `test_model` AS `t1` LIMIT ?;").
					WithArgs(1).
					WillReturnRows(rows)
			},
			wantVal: func() *int {
				val := 10
				return &val
			}(),
		},
		{
			name: "res RawQuery bytes",
			queryRes: func(t *testing.T) any {
				queryer := RawQuery[[]byte](db, "SELECT `first_name` FROM `test_model` WHERE `id`=? LIMIT ?;", 1, 1)
				result, err := queryer.Get(context.Background())
				require.NoError(t, err)
				return result
			},
			mockOrder: func(mock sqlmock.Sqlmock) {
				rows := mock.NewRows([]string{"first_name"}).AddRow([]byte("Li"))
				mock.ExpectQuery("SELECT `first_name` FROM `test_model` WHERE `id`=? LIMIT ?;").
					WithArgs(1, 1).
					WillReturnRows(rows)
			},
			wantVal: func() *[]byte {
				val := []byte("Li")
				return &val
			}(),
		},
		{
			name: "res RawQuery string",
			queryRes: func(t *testing.T) any {
				queryer := RawQuery[string](db, "SELECT `first_name` FROM `test_model` WHERE `id`=? LIMIT ?;", 1, 1)
				result, err := queryer.Get(context.Background())
				require.NoError(t, err)
				return result
			},
			mockOrder: func(mock sqlmock.Sqlmock) {
				rows := mock.NewRows([]string{"first_name"}).AddRow("Da")
				mock.ExpectQuery("SELECT `first_name` FROM `test_model` WHERE `id`=? LIMIT ?;").
					WithArgs(1, 1).
					WillReturnRows(rows)
			},
			wantVal: func() *string {
				val := "Da"
				return &val
			}(),
		},
		{
			name: "res RawQuery sql.NullString",
			queryRes: func(t *testing.T) any {
				queryer := RawQuery[sql.NullString](db, "SELECT `last_name` FROM `test_model` WHERE `id`=? LIMIT ?;", 1, 1)
				result, err := queryer.Get(context.Background())
				require.NoError(t, err)
				return result
			},
			mockOrder: func(mock sqlmock.Sqlmock) {
				rows := mock.NewRows([]string{"last_name"}).AddRow([]byte("ming"))
				mock.ExpectQuery("SELECT `last_name` FROM `test_model` WHERE `id`=? LIMIT ?;").
					WithArgs(1, 1).
					WillReturnRows(rows)
			},
			wantVal: func() *sql.NullString {
				return &sql.NullString{String: "ming", Valid: true}
			}(),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tc.mockOrder(mock)
			res := tc.queryRes(t)
			assert.Equal(t, tc.wantVal, res)
		})
	}
}

func TestRawQuery_GetMulti_baseType(t *testing.T) {
	mockDB, mock, err := sqlmock.New(
		sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = mockDB.Close() }()
	db, err := OpenDS("mysql", single.NewDB(mockDB))
	if err != nil {
		t.Fatal(err)
	}

	testCases := []struct {
		name      string
		queryRes  func(t *testing.T) any
		mockErr   error
		mockOrder func(mock sqlmock.Sqlmock)
		wantErr   error
		wantVal   any
	}{
		{
			name: "res int",
			queryRes: func(t *testing.T) any {
				queryer := RawQuery[int](db, "SELECT `age` FROM `test_model`;")
				result, err := queryer.GetMulti(context.Background())
				require.NoError(t, err)
				return result
			},
			mockOrder: func(mock sqlmock.Sqlmock) {
				rows := mock.NewRows([]string{"age"}).AddRow(10).
					AddRow(18).AddRow(22)
				mock.ExpectQuery("SELECT `age` FROM `test_model`;").
					WillReturnRows(rows)
			},
			wantVal: func() (res []*int) {
				vals := []int{10, 18, 22}
				for i := 0; i < len(vals); i++ {
					res = append(res, &vals[i])
				}
				return
			}(),
		},
		{
			name: "res byte",
			queryRes: func(t *testing.T) any {
				queryer := RawQuery[byte](db, "SELECT `first_name` FROM `test_model`;")
				result, err := queryer.GetMulti(context.Background())
				require.NoError(t, err)
				return result
			},
			mockOrder: func(mock sqlmock.Sqlmock) {
				rows := mock.NewRows([]string{"first_name"}).AddRow('D').AddRow('a')
				mock.ExpectQuery("SELECT `first_name` FROM `test_model`;").
					WillReturnRows(rows)
			},
			wantVal: func() (res []*byte) {
				vals := []byte{'D', 'a'}
				for i := 0; i < len(vals); i++ {
					res = append(res, &vals[i])
				}
				return
			}(),
		},
		{
			name: "res bytes",
			queryRes: func(t *testing.T) any {
				queryer := RawQuery[[]byte](db, "SELECT `first_name` FROM `test_model`;")
				result, err := queryer.GetMulti(context.Background())
				require.NoError(t, err)
				return result
			},
			mockOrder: func(mock sqlmock.Sqlmock) {
				rows := mock.NewRows([]string{"first_name"}).AddRow([]byte("Li")).AddRow([]byte("Liu"))
				mock.ExpectQuery("SELECT `first_name` FROM `test_model`;").
					WillReturnRows(rows)
			},
			wantVal: func() (res []*[]byte) {
				vals := [][]byte{[]byte("Li"), []byte("Liu")}
				for i := 0; i < len(vals); i++ {
					res = append(res, &vals[i])
				}
				return
			}(),
		},
		{
			name: "res string",
			queryRes: func(t *testing.T) any {
				queryer := RawQuery[string](db, "SELECT `first_name` FROM `test_model`;")
				result, err := queryer.GetMulti(context.Background())
				require.NoError(t, err)
				return result
			},
			mockOrder: func(mock sqlmock.Sqlmock) {
				rows := mock.NewRows([]string{"first_name"}).AddRow("Da").AddRow("Li")
				mock.ExpectQuery("SELECT `first_name` FROM `test_model`;").
					WillReturnRows(rows)
			},
			wantVal: func() (res []*string) {
				vals := []string{"Da", "Li"}
				for i := 0; i < len(vals); i++ {
					res = append(res, &vals[i])
				}
				return
			}(),
		},
		{
			name: "res sql.NullString",
			queryRes: func(t *testing.T) any {
				queryer := RawQuery[sql.NullString](db, "SELECT `last_name` FROM `test_model`;")
				result, err := queryer.GetMulti(context.Background())
				require.NoError(t, err)
				return result
			},
			mockOrder: func(mock sqlmock.Sqlmock) {
				rows := mock.NewRows([]string{"last_name"}).
					AddRow([]byte("ming")).AddRow([]byte("gang"))
				mock.ExpectQuery("SELECT `last_name` FROM `test_model`;").
					WillReturnRows(rows)
			},
			wantVal: []*sql.NullString{
				{
					String: "ming",
					Valid:  true,
				},
				{
					String: "gang",
					Valid:  true,
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tc.mockOrder(mock)
			res := tc.queryRes(t)
			assert.EqualValues(t, tc.wantVal, res)
		})
	}
}

func TestSelector_Get_baseType(t *testing.T) {
	mockDB, mock, err := sqlmock.New(
		sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = mockDB.Close() }()
	db, err := OpenDS("mysql", single.NewDB(mockDB))
	if err != nil {
		t.Fatal(err)
	}

	testCases := []struct {
		name      string
		queryRes  func(t *testing.T) (any, error)
		mockErr   error
		mockOrder func(mock sqlmock.Sqlmock)
		wantErr   string
		wantVal   any
	}{
		{
			name: "res int",
			queryRes: func(t *testing.T) (any, error) {
				tm := TableOf(&TestModel{}, "t1")
				queryer := NewSelector[int](db).Select(C("Age")).From(tm)
				return queryer.Get(context.Background())
			},
			mockOrder: func(mock sqlmock.Sqlmock) {
				rows := mock.NewRows([]string{"age"}).AddRow(10)
				mock.ExpectQuery("SELECT `age` FROM `test_model` AS `t1` LIMIT ?;").
					WithArgs(1).
					WillReturnRows(rows)
			},
			wantVal: func() *int {
				val := 10
				return &val
			}(),
		},
		{
			name: "res int32",
			queryRes: func(t *testing.T) (any, error) {
				tm := TableOf(&TestModel{}, "t1")
				queryer := NewSelector[int32](db).Select(C("Age")).From(tm)
				return queryer.Get(context.Background())
			},
			mockOrder: func(mock sqlmock.Sqlmock) {
				rows := mock.NewRows([]string{"age"}).AddRow(10)
				mock.ExpectQuery("SELECT `age` FROM `test_model` AS `t1` LIMIT ?;").
					WithArgs(1).
					WillReturnRows(rows)
			},
			wantVal: func() *int32 {
				val := int32(10)
				return &val
			}(),
		},
		{
			name: "res int64",
			queryRes: func(t *testing.T) (any, error) {
				tm := TableOf(&TestModel{}, "t1")
				queryer := NewSelector[int64](db).Select(C("Age")).From(tm)
				return queryer.Get(context.Background())
			},
			mockOrder: func(mock sqlmock.Sqlmock) {
				rows := mock.NewRows([]string{"age"}).AddRow(10)
				mock.ExpectQuery("SELECT `age` FROM `test_model` AS `t1` LIMIT ?;").
					WithArgs(1).
					WillReturnRows(rows)
			},
			wantVal: func() *int64 {
				val := int64(10)
				return &val
			}(),
		},
		{
			name: "avg res float32",
			queryRes: func(t *testing.T) (any, error) {
				tm := TableOf(&TestModel{}, "t1")
				queryer := NewSelector[float32](db).Select(C("Age")).From(tm)
				return queryer.Get(context.Background())
			},
			mockOrder: func(mock sqlmock.Sqlmock) {
				rows := mock.NewRows([]string{"age"}).AddRow(10.2)
				mock.ExpectQuery("SELECT `age` FROM `test_model` AS `t1` LIMIT ?;").
					WithArgs(1).
					WillReturnRows(rows)
			},
			wantVal: func() *float32 {
				val := float32(10.2)
				return &val
			}(),
		},
		{
			name: "avg res float64",
			queryRes: func(t *testing.T) (any, error) {
				tm := TableOf(&TestModel{}, "t1")
				queryer := NewSelector[float64](db).Select(C("Age")).From(tm)
				return queryer.Get(context.Background())
			},
			mockOrder: func(mock sqlmock.Sqlmock) {
				rows := mock.NewRows([]string{"age"}).AddRow(10.02)
				mock.ExpectQuery("SELECT `age` FROM `test_model` AS `t1` LIMIT ?;").
					WithArgs(1).
					WillReturnRows(rows)
			},
			wantVal: func() *float64 {
				val := 10.02
				return &val
			}(),
		},
		{
			name: "res byte",
			queryRes: func(t *testing.T) (any, error) {
				tm := TableOf(&TestModel{}, "t1")
				queryer := NewSelector[byte](db).Select(C("FirstName")).
					From(tm).Where(C("Id").EQ(1))
				return queryer.Get(context.Background())
			},
			mockOrder: func(mock sqlmock.Sqlmock) {
				rows := mock.NewRows([]string{"first_name"}).AddRow('D')
				mock.ExpectQuery("SELECT `first_name` FROM `test_model` AS `t1` WHERE `id`=? LIMIT ?;").
					WithArgs(1, 1).
					WillReturnRows(rows)
			},
			wantVal: func() *byte {
				val := byte('D')
				return &val
			}(),
		},
		{
			name: "res bytes",
			queryRes: func(t *testing.T) (any, error) {
				tm := TableOf(&TestModel{}, "t1")
				queryer := NewSelector[[]byte](db).Select(C("FirstName")).
					From(tm).Where(C("Id").EQ(1))
				return queryer.Get(context.Background())
			},
			mockOrder: func(mock sqlmock.Sqlmock) {
				rows := mock.NewRows([]string{"first_name"}).AddRow([]byte("Li"))
				mock.ExpectQuery("SELECT `first_name` FROM `test_model` AS `t1` WHERE `id`=? LIMIT ?;").
					WithArgs(1, 1).
					WillReturnRows(rows)
			},
			wantVal: func() *[]byte {
				val := []byte("Li")
				return &val
			}(),
		},
		{
			name: "res string",
			queryRes: func(t *testing.T) (any, error) {
				tm := TableOf(&TestModel{}, "t1")
				queryer := NewSelector[string](db).Select(C("FirstName")).
					From(tm).Where(C("Id").EQ(1))
				return queryer.Get(context.Background())
			},
			mockOrder: func(mock sqlmock.Sqlmock) {
				rows := mock.NewRows([]string{"first_name"}).AddRow("Da")
				mock.ExpectQuery("SELECT `first_name` FROM `test_model` AS `t1` WHERE `id`=? LIMIT ?;").
					WithArgs(1, 1).
					WillReturnRows(rows)
			},
			wantVal: func() *string {
				val := "Da"
				return &val
			}(),
		},
		{
			name: "res struct ptr",
			queryRes: func(t *testing.T) (any, error) {
				queryer := NewSelector[TestModel](db).Select(C("FirstName"), C("Age")).
					Where(C("Id").EQ(1))
				return queryer.Get(context.Background())
			},
			mockOrder: func(mock sqlmock.Sqlmock) {
				rows := mock.NewRows([]string{"first_name", "age"}).AddRow("Da", 18)
				mock.ExpectQuery("SELECT `first_name`,`age` FROM `test_model` WHERE `id`=? LIMIT ?;").
					WithArgs(1, 1).
					WillReturnRows(rows)
			},
			wantVal: func() *TestModel {
				return &TestModel{
					FirstName: "Da",
					Age:       18,
				}
			}(),
		},
		{
			name: "res sql.NullString",
			queryRes: func(t *testing.T) (any, error) {
				tm := TableOf(&TestModel{}, "t1")
				queryer := NewSelector[sql.NullString](db).Select(C("LastName")).
					From(tm).Where(C("Id").EQ(1))
				return queryer.Get(context.Background())
			},
			mockOrder: func(mock sqlmock.Sqlmock) {
				rows := mock.NewRows([]string{"last_name"}).AddRow([]byte("ming"))
				mock.ExpectQuery("SELECT `last_name` FROM `test_model` AS `t1` WHERE `id`=? LIMIT ?;").
					WithArgs(1, 1).
					WillReturnRows(rows)
			},
			wantVal: func() *sql.NullString {
				return &sql.NullString{String: "ming", Valid: true}
			}(),
		},
		{
			name: "res *int accept NULL",
			queryRes: func(t *testing.T) (any, error) {
				tm := TableOf(&TestModel{}, "t1")
				queryer := NewSelector[*int](db).Select(C("Age")).
					From(tm).Where(C("Id").EQ(1))
				return queryer.Get(context.Background())
			},
			mockOrder: func(mock sqlmock.Sqlmock) {
				rows := mock.NewRows([]string{"age"}).AddRow(nil)
				mock.ExpectQuery("SELECT `age` FROM `test_model` AS `t1` WHERE `id`=? LIMIT ?;").
					WithArgs(1, 1).
					WillReturnRows(rows)
			},
			wantVal: func() **int {
				return new(*int)
			}(),
		},
		{
			name: "res int accept NULL",
			queryRes: func(t *testing.T) (any, error) {
				tm := TableOf(&TestModel{}, "t1")
				queryer := NewSelector[int](db).Select(C("Age")).
					From(tm).Where(C("Id").EQ(1))
				return queryer.Get(context.Background())
			},
			mockOrder: func(mock sqlmock.Sqlmock) {
				rows := mock.NewRows([]string{"age"}).AddRow(nil)
				mock.ExpectQuery("SELECT `age` FROM `test_model` AS `t1` WHERE `id`=? LIMIT ?;").
					WithArgs(1, 1).
					WillReturnRows(rows)
			},
			wantErr: "sql: Scan error on column index 0, name \"age\": converting NULL to int is unsupported",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tc.mockOrder(mock)
			res, err := tc.queryRes(t)
			if err != nil {
				assert.EqualError(t, err, tc.wantErr)
				return
			}
			assert.Equal(t, tc.wantVal, res)
		})
	}
}

func TestSelector_GetMulti_baseType(t *testing.T) {
	mockDB, mock, err := sqlmock.New(
		sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = mockDB.Close() }()
	db, err := OpenDS("mysql", single.NewDB(mockDB))
	if err != nil {
		t.Fatal(err)
	}

	testCases := []struct {
		name      string
		queryRes  func(t *testing.T) any
		mockErr   error
		mockOrder func(mock sqlmock.Sqlmock)
		wantErr   error
		wantVal   any
	}{
		{
			name: "res int",
			queryRes: func(t *testing.T) any {
				queryer := NewSelector[int](db).Select(C("Age")).From(TableOf(&TestModel{}, "t1"))
				result, err := queryer.GetMulti(context.Background())
				require.NoError(t, err)
				return result
			},
			mockOrder: func(mock sqlmock.Sqlmock) {
				rows := mock.NewRows([]string{"age"}).AddRow(10).
					AddRow(18).AddRow(22)
				mock.ExpectQuery("SELECT `age` FROM `test_model` AS `t1`;").
					WillReturnRows(rows)
			},
			wantVal: func() (res []*int) {
				vals := []int{10, 18, 22}
				for i := 0; i < len(vals); i++ {
					res = append(res, &vals[i])
				}
				return
			}(),
		},
		{
			name: "res int32",
			queryRes: func(t *testing.T) any {
				queryer := NewSelector[int32](db).Select(C("Age")).From(TableOf(&TestModel{}, "t1"))
				result, err := queryer.GetMulti(context.Background())
				require.NoError(t, err)
				return result
			},
			mockOrder: func(mock sqlmock.Sqlmock) {
				rows := mock.NewRows([]string{"age"}).AddRow(10).
					AddRow(18).AddRow(22)
				mock.ExpectQuery("SELECT `age` FROM `test_model` AS `t1`;").
					WillReturnRows(rows)
			},
			wantVal: func() (res []*int32) {
				vals := []int32{10, 18, 22}
				for i := 0; i < len(vals); i++ {
					res = append(res, &vals[i])
				}
				return
			}(),
		},
		{
			name: "avg res int64",
			queryRes: func(t *testing.T) any {
				queryer := NewSelector[int64](db).Select(C("Age")).From(TableOf(&TestModel{}, "t1"))
				result, err := queryer.GetMulti(context.Background())
				require.NoError(t, err)
				return result
			},
			mockOrder: func(mock sqlmock.Sqlmock) {
				rows := mock.NewRows([]string{"age"}).AddRow(10).
					AddRow(18).AddRow(22)
				mock.ExpectQuery("SELECT `age` FROM `test_model` AS `t1`;").
					WillReturnRows(rows)
			},
			wantVal: func() (res []*int64) {
				vals := []int64{10, 18, 22}
				for i := 0; i < len(vals); i++ {
					res = append(res, &vals[i])
				}
				return
			}(),
		},
		{
			name: "avg res float32",
			queryRes: func(t *testing.T) any {
				queryer := NewSelector[float32](db).Select(C("Age")).From(TableOf(&TestModel{}, "t1"))
				result, err := queryer.GetMulti(context.Background())
				require.NoError(t, err)
				return result
			},
			mockOrder: func(mock sqlmock.Sqlmock) {
				rows := mock.NewRows([]string{"age"}).AddRow(10.2).AddRow(18.8)
				mock.ExpectQuery("SELECT `age` FROM `test_model` AS `t1`;").
					WillReturnRows(rows)
			},
			wantVal: func() (res []*float32) {
				vals := []float32{10.2, 18.8}
				for i := 0; i < len(vals); i++ {
					res = append(res, &vals[i])
				}
				return
			}(),
		},
		{
			name: "avg res float64",
			queryRes: func(t *testing.T) any {
				queryer := NewSelector[float64](db).Select(C("Age")).From(TableOf(&TestModel{}, "t1"))
				result, err := queryer.GetMulti(context.Background())
				require.NoError(t, err)
				return result
			},
			mockOrder: func(mock sqlmock.Sqlmock) {
				rows := mock.NewRows([]string{"age"}).AddRow(10.2).AddRow(18.8)
				mock.ExpectQuery("SELECT `age` FROM `test_model` AS `t1`;").
					WillReturnRows(rows)
			},
			wantVal: func() (res []*float64) {
				vals := []float64{10.2, 18.8}
				for i := 0; i < len(vals); i++ {
					res = append(res, &vals[i])
				}
				return
			}(),
		},
		{
			name: "res byte",
			queryRes: func(t *testing.T) any {
				queryer := NewSelector[byte](db).Select(C("FirstName")).From(TableOf(&TestModel{}, "t1"))
				result, err := queryer.GetMulti(context.Background())
				require.NoError(t, err)
				return result
			},
			mockOrder: func(mock sqlmock.Sqlmock) {
				rows := mock.NewRows([]string{"first_name"}).AddRow('D').AddRow('a')
				mock.ExpectQuery("SELECT `first_name` FROM `test_model` AS `t1`;").
					WillReturnRows(rows)
			},
			wantVal: func() (res []*byte) {
				vals := []byte{'D', 'a'}
				for i := 0; i < len(vals); i++ {
					res = append(res, &vals[i])
				}
				return
			}(),
		},
		{
			name: "res bytes",
			queryRes: func(t *testing.T) any {
				queryer := NewSelector[[]byte](db).Select(C("FirstName")).From(TableOf(&TestModel{}, "t1"))
				result, err := queryer.GetMulti(context.Background())
				require.NoError(t, err)
				return result
			},
			mockOrder: func(mock sqlmock.Sqlmock) {
				rows := mock.NewRows([]string{"first_name"}).AddRow([]byte("Li")).AddRow([]byte("Liu"))
				mock.ExpectQuery("SELECT `first_name` FROM `test_model` AS `t1`;").
					WillReturnRows(rows)
			},
			wantVal: func() (res []*[]byte) {
				vals := [][]byte{[]byte("Li"), []byte("Liu")}
				for i := 0; i < len(vals); i++ {
					res = append(res, &vals[i])
				}
				return
			}(),
		},
		{
			name: "res string",
			queryRes: func(t *testing.T) any {
				queryer := NewSelector[string](db).Select(C("FirstName")).From(TableOf(&TestModel{}, "t1"))
				result, err := queryer.GetMulti(context.Background())
				require.NoError(t, err)
				return result
			},
			mockOrder: func(mock sqlmock.Sqlmock) {
				rows := mock.NewRows([]string{"first_name"}).AddRow("Da").AddRow("Li")
				mock.ExpectQuery("SELECT `first_name` FROM `test_model` AS `t1`;").
					WillReturnRows(rows)
			},
			wantVal: func() (res []*string) {
				vals := []string{"Da", "Li"}
				for i := 0; i < len(vals); i++ {
					res = append(res, &vals[i])
				}
				return
			}(),
		},
		{
			name: "res struct ptr",
			queryRes: func(t *testing.T) any {
				queryer := NewSelector[TestModel](db).Select(C("FirstName"), C("Age")).From(TableOf(&TestModel{}, "t1"))
				result, err := queryer.GetMulti(context.Background())
				require.NoError(t, err)
				return result
			},
			mockOrder: func(mock sqlmock.Sqlmock) {
				rows := mock.NewRows([]string{"first_name", "age"}).
					AddRow("Da", 18).AddRow("Xiao", 16)
				mock.ExpectQuery("SELECT `first_name`,`age` FROM `test_model` AS `t1`;").
					WillReturnRows(rows)
			},
			wantVal: []*TestModel{
				{
					FirstName: "Da",
					Age:       18,
				},
				{
					FirstName: "Xiao",
					Age:       16,
				},
			},
		},
		{
			name: "res sql.NullString",
			queryRes: func(t *testing.T) any {
				queryer := NewSelector[sql.NullString](db).Select(C("LastName")).From(TableOf(&TestModel{}, "t1"))
				result, err := queryer.GetMulti(context.Background())
				require.NoError(t, err)
				return result
			},
			mockOrder: func(mock sqlmock.Sqlmock) {
				rows := mock.NewRows([]string{"last_name"}).
					AddRow([]byte("ming")).AddRow([]byte("gang"))
				mock.ExpectQuery("SELECT `last_name` FROM `test_model` AS `t1`;").
					WillReturnRows(rows)
			},
			wantVal: []*sql.NullString{
				{
					String: "ming",
					Valid:  true,
				},
				{
					String: "gang",
					Valid:  true,
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tc.mockOrder(mock)
			res := tc.queryRes(t)
			assert.EqualValues(t, tc.wantVal, res)
		})
	}
}

func TestSelectable(t *testing.T) {
	db := memoryDB()
	testCases := []CommonTestCase{
		{
			name:    "simple",
			builder: NewSelector[TestModel](db),
			wantSql: "SELECT `id`,`first_name`,`age`,`last_name` FROM `test_model`;",
		},
		{
			name:    "columns",
			builder: NewSelector[TestModel](db).Select(Columns("Id", "FirstName")),
			wantSql: "SELECT `id`,`first_name` FROM `test_model`;",
		},
		{
			name:    "alias",
			builder: NewSelector[TestModel](db).Select(Columns("Id"), C("FirstName").As("name")),
			wantSql: "SELECT `id`,`first_name` AS `name` FROM `test_model`;",
		},
		{
			name:    "aggregate",
			builder: NewSelector[TestModel](db).Select(Columns("Id"), Avg("Age").As("avg_age")),
			wantSql: "SELECT `id`,AVG(`age`) AS `avg_age` FROM `test_model`;",
		},
		{
			name:    "raw",
			builder: NewSelector[TestModel](db).Select(Columns("Id"), Raw("AVG(DISTINCT `age`)")),
			wantSql: "SELECT `id`,AVG(DISTINCT `age`) FROM `test_model`;",
		},
		{
			name:    "invalid columns",
			builder: NewSelector[TestModel](db).Select(Columns("Invalid"), Raw("AVG(DISTINCT `age`)")),
			wantErr: errs.NewInvalidFieldError("Invalid"),
		},
		{
			name:    "order by",
			builder: NewSelector[TestModel](db).OrderBy(ASC("Age"), DESC("Id")),
			wantSql: "SELECT `id`,`first_name`,`age`,`last_name` FROM `test_model` ORDER BY `age` ASC,`id` DESC;",
		},
		{
			name:    "order by invalid column",
			builder: NewSelector[TestModel](db).OrderBy(ASC("Invalid"), DESC("Id")),
			wantErr: errs.NewInvalidFieldError("Invalid"),
		},
		{
			name:    "group by",
			builder: NewSelector[TestModel](db).GroupBy("Age", "Id"),
			wantSql: "SELECT `id`,`first_name`,`age`,`last_name` FROM `test_model` GROUP BY `age`,`id`;",
		},
		{
			name:    "group by invalid column",
			builder: NewSelector[TestModel](db).GroupBy("Invalid", "Id"),
			wantErr: errs.NewInvalidFieldError("Invalid"),
		},
		{
			name:     "offset",
			builder:  NewSelector[TestModel](db).OrderBy(ASC("Age"), DESC("Id")).Offset(10),
			wantSql:  "SELECT `id`,`first_name`,`age`,`last_name` FROM `test_model` ORDER BY `age` ASC,`id` DESC OFFSET ?;",
			wantArgs: []interface{}{10},
		},
		{
			name:     "limit",
			builder:  NewSelector[TestModel](db).OrderBy(ASC("Age"), DESC("Id")).Offset(10).Limit(100),
			wantSql:  "SELECT `id`,`first_name`,`age`,`last_name` FROM `test_model` ORDER BY `age` ASC,`id` DESC OFFSET ? LIMIT ?;",
			wantArgs: []interface{}{10, 100},
		},
		{
			name:     "where",
			builder:  NewSelector[TestModel](db).Where(C("Id").EQ(10)),
			wantSql:  "SELECT `id`,`first_name`,`age`,`last_name` FROM `test_model` WHERE `id`=?;",
			wantArgs: []interface{}{10},
		},
		{
			name:    "no where",
			builder: NewSelector[TestModel](db).Where(),
			wantSql: "SELECT `id`,`first_name`,`age`,`last_name` FROM `test_model`;",
		},
		{
			name:     "having",
			builder:  NewSelector[TestModel](db).GroupBy("FirstName").Having(Avg("Age").EQ(18)),
			wantSql:  "SELECT `id`,`first_name`,`age`,`last_name` FROM `test_model` GROUP BY `first_name` HAVING AVG(`age`)=?;",
			wantArgs: []interface{}{18},
		},
		{
			name:    "no having",
			builder: NewSelector[TestModel](db).GroupBy("FirstName").Having(),
			wantSql: "SELECT `id`,`first_name`,`age`,`last_name` FROM `test_model` GROUP BY `first_name`;",
		},
		{
			name:     "alias in having",
			builder:  NewSelector[TestModel](db).Select(Columns("Id"), Columns("FirstName"), Avg("Age").As("avg_age")).GroupBy("FirstName").Having(Avg("Age").LT(20)),
			wantSql:  "SELECT `id`,`first_name`,AVG(`age`) AS `avg_age` FROM `test_model` GROUP BY `first_name` HAVING AVG(`age`)<?;",
			wantArgs: []interface{}{20},
		},
		{
			name:    "invalid alias in having",
			builder: NewSelector[TestModel](db).Select(Columns("Id"), Columns("FirstName"), Avg("Age").As("avg_age")).GroupBy("FirstName").Having(C("Invalid").LT(20)),
			wantErr: errs.NewInvalidFieldError("Invalid"),
		},
		{
			name:     "in",
			builder:  NewSelector[TestModel](db).Select(Columns("Id")).Where(C("Id").In(1, 2, 3)),
			wantSql:  "SELECT `id` FROM `test_model` WHERE `id` IN (?,?,?);",
			wantArgs: []interface{}{1, 2, 3},
		},
		{
			name:     "not in",
			builder:  NewSelector[TestModel](db).Select(Columns("Id")).Where(C("Id").NotIn(1, 2, 3)),
			wantSql:  "SELECT `id` FROM `test_model` WHERE `id` NOT IN (?,?,?);",
			wantArgs: []interface{}{1, 2, 3},
		},
		{
			// 传入的参数为切片
			name:     "slice in",
			builder:  NewSelector[TestModel](db).Select(Columns("Id")).Where(C("Id").In([]int{1, 2, 3})),
			wantSql:  "SELECT `id` FROM `test_model` WHERE `id` IN (?);",
			wantArgs: []interface{}{[]int{1, 2, 3}},
		},
		{
			// in 后面没有值
			name:    "no in",
			builder: NewSelector[TestModel](db).Select(Columns("Id")).Where(C("Id").In()),
			wantSql: "SELECT `id` FROM `test_model` WHERE FALSE;",
		},
		{
			// Notin 后面没有值
			name:    "no in",
			builder: NewSelector[TestModel](db).Select(Columns("Id")).Where(C("Id").NotIn()),
			wantSql: "SELECT `id` FROM `test_model` WHERE FALSE;",
		},
		{
			name:    "in empty slice",
			builder: NewSelector[TestModel](db).Select(Columns("Id")).Where(C("Id").In([]any{}...)),
			wantSql: "SELECT `id` FROM `test_model` WHERE FALSE;",
		},
		{
			name:    "NOT In empty slice",
			builder: NewSelector[TestModel](db).Select(Columns("Id")).Where(C("Id").NotIn([]any{}...)),
			wantSql: "SELECT `id` FROM `test_model` WHERE FALSE;",
		},
		// 模糊查询
		{
			name:    "NOT In empty slice",
			builder: NewSelector[TestModel](db).Select(Columns("Id")).Where(C("Id").NotIn([]any{}...)),
			wantSql: "SELECT `id` FROM `test_model` WHERE FALSE;",
		},
		{
			name:     "where not like %",
			builder:  NewSelector[TestModel](db).Where(C("FirstName").NotLike("%ming")),
			wantSql:  "SELECT `id`,`first_name`,`age`,`last_name` FROM `test_model` WHERE `first_name` NOT LIKE ?;",
			wantArgs: []interface{}{"%ming"},
		},
		{
			name:     "where like %",
			builder:  NewSelector[TestModel](db).Where(C("FirstName").Like("zhang%")),
			wantSql:  "SELECT `id`,`first_name`,`age`,`last_name` FROM `test_model` WHERE `first_name` LIKE ?;",
			wantArgs: []interface{}{"zhang%"},
		},
		{
			name:     "where not like _",
			builder:  NewSelector[TestModel](db).Where(C("FirstName").NotLike("_三_")),
			wantSql:  "SELECT `id`,`first_name`,`age`,`last_name` FROM `test_model` WHERE `first_name` NOT LIKE ?;",
			wantArgs: []interface{}{"_三_"},
		},
		{
			name:     "where like _",
			builder:  NewSelector[TestModel](db).Where(C("FirstName").Like("_三_")),
			wantSql:  "SELECT `id`,`first_name`,`age`,`last_name` FROM `test_model` WHERE `first_name` LIKE ?;",
			wantArgs: []interface{}{"_三_"},
		},
		{
			name:     "where not like []",
			builder:  NewSelector[TestModel](db).Where(C("FirstName").NotLike("老[1-9]")),
			wantSql:  "SELECT `id`,`first_name`,`age`,`last_name` FROM `test_model` WHERE `first_name` NOT LIKE ?;",
			wantArgs: []interface{}{"老[1-9]"},
		},
		{
			name:     "where like []",
			builder:  NewSelector[TestModel](db).Where(C("FirstName").Like("老[1-9]")),
			wantSql:  "SELECT `id`,`first_name`,`age`,`last_name` FROM `test_model` WHERE `first_name` LIKE ?;",
			wantArgs: []interface{}{"老[1-9]"},
		},
		{
			name:     "where not like [^ ]",
			builder:  NewSelector[TestModel](db).Where(C("FirstName").NotLike("老[^1-4]")),
			wantSql:  "SELECT `id`,`first_name`,`age`,`last_name` FROM `test_model` WHERE `first_name` NOT LIKE ?;",
			wantArgs: []interface{}{"老[^1-4]"},
		},
		{
			name:     "where like [^ ]",
			builder:  NewSelector[TestModel](db).Where(C("FirstName").Like("老[^1-4]")),
			wantSql:  "SELECT `id`,`first_name`,`age`,`last_name` FROM `test_model` WHERE `first_name` LIKE ?;",
			wantArgs: []interface{}{"老[^1-4]"},
		},

		{
			name:     "where not like int",
			builder:  NewSelector[TestModel](db).Where(C("Age").NotLike(18)),
			wantSql:  "SELECT `id`,`first_name`,`age`,`last_name` FROM `test_model` WHERE `age` NOT LIKE ?;",
			wantArgs: []interface{}{18},
		},
		{
			name:     "where like int",
			builder:  NewSelector[TestModel](db).Where(C("Age").Like(22)),
			wantSql:  "SELECT `id`,`first_name`,`age`,`last_name` FROM `test_model` WHERE `age` LIKE ?;",
			wantArgs: []interface{}{22},
		},
		{
			name:     "having like %",
			builder:  NewSelector[TestModel](db).GroupBy("FirstName").Having(C("LastName").Like("%li")),
			wantSql:  "SELECT `id`,`first_name`,`age`,`last_name` FROM `test_model` GROUP BY `first_name` HAVING `last_name` LIKE ?;",
			wantArgs: []interface{}{"%li"},
		},
		{
			name:     "having no like %",
			builder:  NewSelector[TestModel](db).GroupBy("FirstName").Having(C("LastName").NotLike("%yy%")),
			wantSql:  "SELECT `id`,`first_name`,`age`,`last_name` FROM `test_model` GROUP BY `first_name` HAVING `last_name` NOT LIKE ?;",
			wantArgs: []interface{}{"%yy%"},
		},
		{
			name:    "distinct single row",
			builder: NewSelector[TestModel](db).Distinct().Select(C("FirstName")),
			wantSql: "SELECT DISTINCT `first_name` FROM `test_model`;",
		},
		{
			name:    "count distinct",
			builder: NewSelector[TestModel](db).Select(CountDistinct("FirstName")),
			wantSql: "SELECT COUNT(DISTINCT `first_name`) FROM `test_model`;",
		},
		{
			name:     "having count distinct",
			builder:  NewSelector[TestModel](db).Select(C("FirstName")).GroupBy("FirstName").Having(CountDistinct("FirstName").EQ("jack")),
			wantSql:  "SELECT `first_name` FROM `test_model` GROUP BY `first_name` HAVING COUNT(DISTINCT `first_name`)=?;",
			wantArgs: []interface{}{"jack"},
		},
	}

	for _, tc := range testCases {
		c := tc
		t.Run(c.name, func(t *testing.T) {
			query, err := c.builder.Build()
			assert.Equal(t, c.wantErr, err)
			if err != nil {
				return
			}
			assert.Equal(t, c.wantSql, query.SQL)
			assert.Equal(t, c.wantArgs, query.Args)
		})
	}
}

func TestSelectableCombination(t *testing.T) {
	db, err := Open("sqlite3", "file:test.db?cache=shared&mode=memory")
	if err != nil {
		t.Error(err)
	}
	testCases := []CommonTestCase{
		{
			name:    "simple",
			builder: NewSelector[TestCombinedModel](db),
			wantSql: "SELECT `create_time`,`update_time`,`id`,`first_name`,`age`,`last_name` FROM `test_combined_model`;",
		},
		{
			name:    "columns",
			builder: NewSelector[TestCombinedModel](db).Select(Columns("Id", "FirstName", "CreateTime")),
			wantSql: "SELECT `id`,`first_name`,`create_time` FROM `test_combined_model`;",
		},
		{
			name:    "alias",
			builder: NewSelector[TestCombinedModel](db).Select(Columns("Id"), C("CreateTime").As("creation")),
			wantSql: "SELECT `id`,`create_time` AS `creation` FROM `test_combined_model`;",
		},
		{
			name:    "aggregate",
			builder: NewSelector[TestCombinedModel](db).Select(Columns("Id"), Max("CreateTime").As("max_time")),
			wantSql: "SELECT `id`,MAX(`create_time`) AS `max_time` FROM `test_combined_model`;",
		},
		{
			name:    "raw",
			builder: NewSelector[TestCombinedModel](db).Select(Columns("Id"), Raw("AVG(DISTINCT `create_time`)")),
			wantSql: "SELECT `id`,AVG(DISTINCT `create_time`) FROM `test_combined_model`;",
		},
		{
			name:    "invalid columns",
			builder: NewSelector[TestCombinedModel](db).Select(Columns("Invalid"), Raw("AVG(DISTINCT `age`)")),
			wantErr: errs.NewInvalidFieldError("Invalid"),
		},
		{
			name:    "order by",
			builder: NewSelector[TestCombinedModel](db).OrderBy(ASC("Age"), DESC("CreateTime")),
			wantSql: "SELECT `create_time`,`update_time`,`id`,`first_name`,`age`,`last_name` FROM `test_combined_model` ORDER BY `age` ASC,`create_time` DESC;",
		},
		{
			name:    "order by invalid column",
			builder: NewSelector[TestCombinedModel](db).OrderBy(ASC("Invalid"), DESC("Id")),
			wantErr: errs.NewInvalidFieldError("Invalid"),
		},
		{
			name:    "group by",
			builder: NewSelector[TestCombinedModel](db).GroupBy("CreateTime", "Id"),
			wantSql: "SELECT `create_time`,`update_time`,`id`,`first_name`,`age`,`last_name` FROM `test_combined_model` GROUP BY `create_time`,`id`;",
		},
		{
			name:    "group by invalid column",
			builder: NewSelector[TestCombinedModel](db).GroupBy("Invalid", "Id"),
			wantErr: errs.NewInvalidFieldError("Invalid"),
		},
		{
			name:     "offset",
			builder:  NewSelector[TestCombinedModel](db).OrderBy(ASC("Age"), DESC("CreateTime")).Offset(10),
			wantSql:  "SELECT `create_time`,`update_time`,`id`,`first_name`,`age`,`last_name` FROM `test_combined_model` ORDER BY `age` ASC,`create_time` DESC OFFSET ?;",
			wantArgs: []interface{}{10},
		},
		{
			name:     "limit",
			builder:  NewSelector[TestCombinedModel](db).OrderBy(ASC("Age"), DESC("CreateTime")).Offset(10).Limit(100),
			wantSql:  "SELECT `create_time`,`update_time`,`id`,`first_name`,`age`,`last_name` FROM `test_combined_model` ORDER BY `age` ASC,`create_time` DESC OFFSET ? LIMIT ?;",
			wantArgs: []interface{}{10, 100},
		},
		{
			name:     "where",
			builder:  NewSelector[TestCombinedModel](db).Where(C("Id").EQ(10).And(C("CreateTime").EQ(10))),
			wantSql:  "SELECT `create_time`,`update_time`,`id`,`first_name`,`age`,`last_name` FROM `test_combined_model` WHERE (`id`=?) AND (`create_time`=?);",
			wantArgs: []interface{}{10, 10},
		},
		{
			name:    "no where",
			builder: NewSelector[TestCombinedModel](db).Where(),
			wantSql: "SELECT `create_time`,`update_time`,`id`,`first_name`,`age`,`last_name` FROM `test_combined_model`;",
		},
		{
			name:     "having",
			builder:  NewSelector[TestCombinedModel](db).GroupBy("FirstName").Having(Max("CreateTime").EQ(18)),
			wantSql:  "SELECT `create_time`,`update_time`,`id`,`first_name`,`age`,`last_name` FROM `test_combined_model` GROUP BY `first_name` HAVING MAX(`create_time`)=?;",
			wantArgs: []interface{}{18},
		},
		{
			name:    "no having",
			builder: NewSelector[TestCombinedModel](db).GroupBy("CreateTime").Having(),
			wantSql: "SELECT `create_time`,`update_time`,`id`,`first_name`,`age`,`last_name` FROM `test_combined_model` GROUP BY `create_time`;",
		},
		{
			name:     "alias in having",
			builder:  NewSelector[TestCombinedModel](db).Select(Columns("Id"), Columns("FirstName"), Avg("CreateTime").As("create")).GroupBy("FirstName").Having(Avg("CreateTime").LT(20)),
			wantSql:  "SELECT `id`,`first_name`,AVG(`create_time`) AS `create` FROM `test_combined_model` GROUP BY `first_name` HAVING AVG(`create_time`)<?;",
			wantArgs: []interface{}{20},
		},
		{
			name:    "invalid alias in having",
			builder: NewSelector[TestCombinedModel](db).Select(Columns("Id"), Columns("FirstName"), Avg("Age").As("avg_age")).GroupBy("FirstName").Having(C("Invalid").LT(20)),
			wantErr: errs.NewInvalidFieldError("Invalid"),
		},
	}

	for _, tc := range testCases {
		c := tc
		t.Run(c.name, func(t *testing.T) {
			query, err := c.builder.Build()
			assert.Equal(t, c.wantErr, err)
			if err != nil {
				return
			}
			assert.Equal(t, c.wantSql, query.SQL)
			assert.Equal(t, c.wantArgs, query.Args)
		})
	}
}

type BaseEntity struct {
	CreateTime uint64
	UpdateTime uint64
}

type TestCombinedModel struct {
	BaseEntity
	Id        int64 `eorm:"primary_key"`
	FirstName string
	Age       int8
	LastName  *string
}

func ExampleSelector_OrderBy() {
	db, _ := Open("sqlite3", "file:test.db?cache=shared&mode=memory")
	query, _ := NewSelector[TestModel](db).OrderBy(ASC("Age")).Build()
	fmt.Printf("case1\n%s", query.String())
	query, _ = NewSelector[TestModel](db).OrderBy(ASC("Age", "Id")).Build()
	fmt.Printf("case2\n%s", query.String())
	query, _ = NewSelector[TestModel](db).OrderBy(ASC("Age"), ASC("Id")).Build()
	fmt.Printf("case3\n%s", query.String())
	query, _ = NewSelector[TestModel](db).OrderBy(ASC("Age"), DESC("Id")).Build()
	fmt.Printf("case4\n%s", query.String())
	// Output:
	// case1
	// SQL: SELECT `id`,`first_name`,`age`,`last_name` FROM `test_model` ORDER BY `age` ASC;
	// Args: []interface {}(nil)
	// case2
	// SQL: SELECT `id`,`first_name`,`age`,`last_name` FROM `test_model` ORDER BY `age``id` ASC;
	// Args: []interface {}(nil)
	// case3
	// SQL: SELECT `id`,`first_name`,`age`,`last_name` FROM `test_model` ORDER BY `age` ASC,`id` ASC;
	// Args: []interface {}(nil)
	// case4
	// SQL: SELECT `id`,`first_name`,`age`,`last_name` FROM `test_model` ORDER BY `age` ASC,`id` DESC;
	// Args: []interface {}(nil)
}

func ExampleSelector_Having() {
	db := memoryDB()
	query, _ := NewSelector[TestModel](db).Select(Columns("Id"), Columns("FirstName"), Avg("Age").As("avg_age")).GroupBy("FirstName").Having(Avg("Age").LT(20)).Build()
	fmt.Printf("case1\n%s", query.String())
	query, err := NewSelector[TestModel](db).Select(Columns("Id"), Columns("FirstName"), Avg("Age").As("avg_age")).GroupBy("FirstName").Having(C("Invalid").LT(20)).Build()
	fmt.Printf("case2\n%s", err)
	// Output:
	// case1
	// SQL: SELECT `id`,`first_name`,AVG(`age`) AS `avg_age` FROM `test_model` GROUP BY `first_name` HAVING AVG(`age`)<?;
	// Args: []interface {}{20}
	// case2
	// eorm: 未知字段 Invalid
}

func ExampleSelector_Select() {
	db := memoryDB()
	tm := TableOf(&TestModel{}, "t1")
	cases := []*Selector[TestModel]{
		// case0: all columns are included
		NewSelector[TestModel](db).From(tm),
		// case1: only query specific columns
		NewSelector[TestModel](db).Select(Columns("Id", "Age")).From(tm),
		// case2: using alias
		NewSelector[TestModel](db).Select(C("Id").As("my_id")).From(tm),
		// case3: using aggregation function and alias
		NewSelector[TestModel](db).Select(Avg("Age").As("avg_age")).From(tm),
		// case4: using raw expression
		NewSelector[TestModel](db).Select(Raw("COUNT(DISTINCT `age`) AS `age_cnt`")).From(tm),
	}

	for index, tc := range cases {
		query, _ := tc.Build()
		fmt.Printf("case%d:\n%s", index, query.String())
	}
	// Output:
	// case0:
	// SQL: SELECT `id`,`first_name`,`age`,`last_name` FROM `test_model` AS `t1`;
	// Args: []interface {}(nil)
	// case1:
	// SQL: SELECT `id`,`age` FROM `test_model` AS `t1`;
	// Args: []interface {}(nil)
	// case2:
	// SQL: SELECT `id` AS `my_id` FROM `test_model` AS `t1`;
	// Args: []interface {}(nil)
	// case3:
	// SQL: SELECT AVG(`age`) AS `avg_age` FROM `test_model` AS `t1`;
	// Args: []interface {}(nil)
	// case4:
	// SQL: SELECT COUNT(DISTINCT `age`) AS `age_cnt` FROM `test_model` AS `t1`;
	// Args: []interface {}(nil)
}

func ExampleSelector_Distinct() {
	db := memoryDB()
	cases := []*Selector[TestModel]{
		// case0: disinct column
		NewSelector[TestModel](db).Distinct().Select(C("FirstName")),
		// case1: aggregation function using distinct
		NewSelector[TestModel](db).Select(CountDistinct("FirstName")),
		// case2: having using distinct
		NewSelector[TestModel](db).Select(C("FirstName")).GroupBy("FirstName").Having(CountDistinct("FirstName").EQ("jack")),
	}

	for index, tc := range cases {
		query, _ := tc.Build()
		fmt.Printf("case%d:\n%s", index, query.String())
	}
	// Output:
	// case0:
	// SQL: SELECT DISTINCT `first_name` FROM `test_model`;
	// Args: []interface {}(nil)
	// case1:
	// SQL: SELECT COUNT(DISTINCT `first_name`) FROM `test_model`;
	// Args: []interface {}(nil)
	// case2:
	// SQL: SELECT `first_name` FROM `test_model` GROUP BY `first_name` HAVING COUNT(DISTINCT `first_name`)=?;
	// Args: []interface {}{"jack"}
}

func TestSelector_Join(t *testing.T) {
	db := memoryDB()
	type Order struct {
		Id        int
		UsingCol1 string
		UsingCol2 string
	}

	type OrderDetail struct {
		OrderId   int
		ItemId    int
		UsingCol1 string
		UsingCol2 string
	}

	type Item struct {
		Id int
	}

	testCases := []struct {
		name      string
		s         QueryBuilder
		wantQuery Query
		wantErr   error
	}{
		{
			name: "specify table",
			s:    NewSelector[Order](db).From(TableOf(&OrderDetail{}, "t1")),
			wantQuery: Query{
				SQL: "SELECT `order_id`,`item_id`,`using_col1`,`using_col2` FROM `order_detail` AS `t1`;",
			},
		},
		{
			name: "specify table with empty alias",
			s:    NewSelector[Order](db).From(TableOf(&OrderDetail{}, "")),
			wantQuery: Query{
				SQL: "SELECT `order_id`,`item_id`,`using_col1`,`using_col2` FROM `order_detail`;",
			},
		},
		{
			name: "only NewSelector",
			s:    NewSelector[Order](db),
			wantQuery: Query{
				SQL: "SELECT `id`,`using_col1`,`using_col2` FROM `order`;",
			},
		},
		{
			name: "join-using",
			s: func() QueryBuilder {
				t1 := TableOf(&Order{}, "t1")
				t2 := TableOf(&OrderDetail{}, "t2")
				t3 := t1.Join(t2).Using("UsingCol1", "UsingCol2")
				return NewSelector[Order](db).Select(Raw("*")).From(t3)
			}(),
			wantQuery: Query{
				SQL: "SELECT * FROM (`order` AS `t1` JOIN `order_detail` AS `t2` USING (`using_col1`,`using_col2`));",
			},
		},
		{
			name: "join-using-cols",
			s: func() QueryBuilder {
				t1 := TableOf(&Order{}, "t1")
				t2 := TableOf(&OrderDetail{}, "t2")
				t3 := t1.Join(t2).Using("UsingCol1", "UsingCol2")
				return NewSelector[Order](db).From(t3).Select(t1.C("UsingCol1"), t2.C("UsingCol1"))
			}(),
			wantQuery: Query{
				SQL: "SELECT `t1`.`using_col1`,`t2`.`using_col1` FROM (`order` AS `t1` JOIN `order_detail` AS `t2` USING (`using_col1`,`using_col2`));",
			},
		},
		{
			name: "join-using-cols-invalid",
			s: func() QueryBuilder {
				t1 := TableOf(&Order{}, "t1")
				t2 := TableOf(&OrderDetail{}, "t2")
				t3 := t1.Join(t2).Using("invalid", "invalid2")
				return NewSelector[Order](db).From(t3).Select(t1.C("UsingCol2"))
			}(),
			wantErr: errs.NewInvalidFieldError("invalid"),
		},
		{
			name: "join-using-cols-Avg",
			s: func() QueryBuilder {
				t1 := TableOf(&Order{}, "t1")
				t2 := TableOf(&OrderDetail{}, "t2")
				t3 := t1.Join(t2).Using("UsingCol1", "UsingCol2")
				return NewSelector[Order](db).From(t3).Select(t1.Avg("UsingCol1").As("avg_using_col1"))
			}(),
			wantQuery: Query{
				SQL: "SELECT AVG(`t1`.`using_col1`) AS `avg_using_col1` FROM (`order` AS `t1` JOIN `order_detail` AS `t2` USING (`using_col1`,`using_col2`));",
			},
		},
		{
			name: "join-using-Avg-invalid",
			s: func() QueryBuilder {
				t1 := TableOf(&Order{}, "t1")
				t2 := TableOf(&OrderDetail{}, "t2")
				t3 := t1.Join(t2).Using("UsingCol1", "UsingCol2")
				return NewSelector[Order](db).From(t3).Select(t1.Avg("invalid"))
			}(),
			wantErr: errs.NewInvalidFieldError("invalid"),
		},
		{
			name: "join-using-where As",
			s: func() QueryBuilder {
				t1 := TableOf(&Order{}, "t1")
				t2 := TableOf(&OrderDetail{}, "t2")
				t3 := t1.Join(t2).Using("UsingCol1", "UsingCol2")
				return NewSelector[Order](db).Select(t1.AllColumns()).From(t3).Where(C("UsingCol1").EQ(10).And(C("UsingCol2").EQ(10)))
			}(),
			wantQuery: Query{
				SQL:  "SELECT `t1`.* FROM (`order` AS `t1` JOIN `order_detail` AS `t2` USING (`using_col1`,`using_col2`)) WHERE (`using_col1`=?) AND (`using_col2`=?);",
				Args: []interface{}{10, 10},
			},
		},
		{
			name: "join-on",
			s: func() QueryBuilder {
				t1 := TableOf(&Order{}, "t1")
				t2 := TableOf(&OrderDetail{}, "t2")
				t3 := t1.Join(t2).On(t1.C("Id").EQ(t2.C("OrderId")))
				return NewSelector[Order](db).Select(t1.AllColumns()).From(t3)
			}(),
			wantQuery: Query{
				SQL: "SELECT `t1`.* FROM (`order` AS `t1` JOIN `order_detail` AS `t2` ON `t1`.`id`=`t2`.`order_id`);",
			},
		},
		{
			name: "join-on-where As",
			s: func() QueryBuilder {
				t1 := TableOf(&Order{}, "t1")
				t2 := TableOf(&OrderDetail{}, "t2")
				t3 := t1.Join(t2).On(t1.C("Id").EQ(t2.C("OrderId")))
				return NewSelector[Order](db).Select(t1.AllColumns()).From(t3).Where(C("UsingCol1").EQ(10).And(C("UsingCol2").EQ(10)))
			}(),
			wantQuery: Query{
				SQL:  "SELECT `t1`.* FROM (`order` AS `t1` JOIN `order_detail` AS `t2` ON `t1`.`id`=`t2`.`order_id`) WHERE (`using_col1`=?) AND (`using_col2`=?);",
				Args: []interface{}{10, 10},
			},
		},
		{
			name: "join-on-where-invalid-clos",
			s: func() QueryBuilder {
				t1 := TableOf(&Order{}, "t1")
				t2 := TableOf(&OrderDetail{}, "t2")
				t3 := t1.Join(t2).On(t1.C("Id").EQ(t2.C("OrderId")))
				return NewSelector[Order](db).From(t3).Select(t1.C("invalid")).Where(C("invalid").EQ(10).And(C("UsingCol2").EQ(10)))
			}(),
			wantErr: errs.NewInvalidFieldError("invalid"),
		},
		{
			name: "join-on-where-invalid-Min-clos",
			s: func() QueryBuilder {
				t1 := TableOf(&Order{}, "t1")
				t2 := TableOf(&OrderDetail{}, "t2")
				t3 := t1.Join(t2).On(t1.C("Id").EQ(t2.C("OrderId")))
				return NewSelector[Order](db).From(t3).Select(t1.Min("invalid"), t1.C("invalid")).Where(C("invalid").EQ(10).And(C("UsingCol2").EQ(10)))
			}(),
			wantErr: errs.NewInvalidFieldError("invalid"),
		},
		{
			// SELECT MAX(t1.xxx), t2.xxx
			name: "join-on-where-Max-clos",
			s: func() QueryBuilder {
				t1 := TableOf(&Order{}, "t1")
				t2 := TableOf(&OrderDetail{}, "t2")
				t3 := t1.LeftJoin(t2).On(t1.C("Id").EQ(t2.C("OrderId")))
				return NewSelector[Order](db).From(t3).Select(t1.Max("UsingCol1").As("UsingCol1"), t1.C("UsingCol2")).Where(t1.C("UsingCol2").EQ("UsingCol2_1").And(t1.C("UsingCol2").EQ("UsingCol2_2")))
			}(),
			wantQuery: Query{
				SQL:  "SELECT MAX(`t1`.`using_col1`) AS `UsingCol1`,`t1`.`using_col2` FROM (`order` AS `t1` LEFT JOIN `order_detail` AS `t2` ON `t1`.`id`=`t2`.`order_id`) WHERE (`t1`.`using_col2`=?) AND (`t1`.`using_col2`=?);",
				Args: []interface{}{"UsingCol2_1", "UsingCol2_2"}},
		},
		{
			name: "join table",
			s: func() QueryBuilder {
				t1 := TableOf(&Order{}, "t1")
				t2 := TableOf(&OrderDetail{}, "t2")
				t3 := t1.Join(t2).On(t1.C("Id").EQ(t2.C("OrderId")))
				t4 := TableOf(&Item{}, "t4")
				t5 := t3.Join(t4).On(t2.C("ItemId").EQ(t4.C("Id")))
				return NewSelector[Order](db).Select(t1.AllColumns()).From(t5)
			}(),
			wantQuery: Query{
				SQL: "SELECT `t1`.* FROM " +
					"((`order` AS `t1` JOIN `order_detail` AS `t2` ON `t1`.`id`=`t2`.`order_id`) " +
					"JOIN `item` AS `t4` ON `t2`.`item_id`=`t4`.`id`);",
			},
		},
		{
			name: "join table-right",
			s: func() QueryBuilder {
				t1 := TableOf(&Order{}, "t1")
				t2 := TableOf(&OrderDetail{}, "t2")
				t3 := t1.Join(t2).On(t1.C("Id").EQ(t2.C("OrderId")))
				t4 := TableOf(&Item{}, "t4")
				t5 := t3.RightJoin(t4).On(t2.C("ItemId").EQ(t4.C("Id")))
				return NewSelector[Order](db).Select(t1.AllColumns()).From(t5)
			}(),
			wantQuery: Query{
				SQL: "SELECT `t1`.* FROM ((`order` AS `t1` JOIN `order_detail` AS `t2` ON `t1`.`id`=`t2`.`order_id`) RIGHT JOIN `item` AS `t4` ON `t2`.`item_id`=`t4`.`id`);",
			},
		},
		{
			name: "join table-left",
			s: func() QueryBuilder {
				t1 := TableOf(&Order{}, "t1")
				t2 := TableOf(&OrderDetail{}, "t2")
				t3 := t1.Join(t2).On(t1.C("Id").EQ(t2.C("OrderId")))
				t4 := TableOf(&Item{}, "t4")
				t5 := t3.LeftJoin(t4).On(t2.C("ItemId").EQ(t4.C("Id")))
				return NewSelector[Order](db).Select(t1.AllColumns()).From(t5)
			}(),
			wantQuery: Query{
				SQL: "SELECT `t1`.* FROM ((`order` AS `t1` JOIN `order_detail` AS `t2` ON `t1`.`id`=`t2`.`order_id`) LEFT JOIN `item` AS `t4` ON `t2`.`item_id`=`t4`.`id`);",
			},
		},
		{
			// SELECT AVG(t1.xxx), AVG(t2.xxx)
			name: "join table AVG-AVG ",
			s: func() QueryBuilder {
				t1 := TableOf(&Order{}, "t1")
				t2 := TableOf(&OrderDetail{}, "t2")
				t3 := t1.Join(t2).On(t1.C("Id").EQ(t2.C("OrderId")))
				t4 := TableOf(&Item{}, "t4")
				t5 := t3.Join(t4).On(t2.C("ItemId").EQ(t4.C("Id")))
				return NewSelector[Order](db).From(t5).Select(t1.Avg("UsingCol1").As("UsingCol1"), t1.Avg("UsingCol2").As("UsingCol2"))
			}(),
			wantQuery: Query{
				SQL: "SELECT AVG(`t1`.`using_col1`) AS `UsingCol1`,AVG(`t1`.`using_col2`) AS `UsingCol2` FROM ((`order` AS `t1` JOIN `order_detail` AS `t2` ON `t1`.`id`=`t2`.`order_id`) JOIN `item` AS `t4` ON `t2`.`item_id`=`t4`.`id`);",
			},
		},
		{
			// SELECT AVG(t1.xxx), AVG(t2.xxx)
			name: "join table AVG-AVG invalid ",
			s: func() QueryBuilder {
				t1 := TableOf(&Order{}, "t1")
				t2 := TableOf(&OrderDetail{}, "t2")
				t3 := t1.Join(t2).On(t1.C("Id").EQ(t2.C("OrderId")))
				t4 := TableOf(&Item{}, "t4")
				t5 := t3.Join(t4).On(t2.C("ItemId").EQ(t4.C("Id")))
				return NewSelector[Order](db).From(t5).Select(t1.Avg("invalid"), t1.Avg("invalid"))
			}(),
			wantErr: errs.NewInvalidFieldError("invalid"),
		},
		{
			// SELECT t1.xxx, t2.xxx
			name: "join table C-C ",
			s: func() QueryBuilder {
				t1 := TableOf(&Order{}, "t1")
				t2 := TableOf(&OrderDetail{}, "t2")
				t3 := t1.Join(t2).On(t1.C("Id").EQ(t2.C("OrderId")))
				t4 := TableOf(&Item{}, "t4")
				t5 := t3.Join(t4).On(t2.C("ItemId").EQ(t4.C("Id")))
				return NewSelector[Order](db).From(t5).Select(t1.C("UsingCol1"), t1.C("UsingCol2"))
			}(),
			wantQuery: Query{
				SQL: "SELECT `t1`.`using_col1`,`t1`.`using_col2` FROM " +
					"((`order` AS `t1` JOIN `order_detail` AS `t2` ON `t1`.`id`=`t2`.`order_id`) " +
					"JOIN `item` AS `t4` ON `t2`.`item_id`=`t4`.`id`);",
			},
		},
		{
			// SELECT t1.xxx, t2.xxx
			name: "join table C-C invalid",
			s: func() QueryBuilder {
				t1 := TableOf(&Order{}, "t1")
				t2 := TableOf(&OrderDetail{}, "t2")
				t3 := t1.Join(t2).On(t1.C("Id").EQ(t2.C("OrderId")))
				t4 := TableOf(&Item{}, "t4")
				t5 := t3.Join(t4).On(t2.C("ItemId").EQ(t4.C("Id")))
				return NewSelector[Order](db).From(t5).Select(t1.C("invalid"), t1.C("invalid"))
			}(),
			wantErr: errs.NewInvalidFieldError("invalid"),
		},
		{
			name: "table join",
			s: func() QueryBuilder {
				t1 := TableOf(&Order{}, "t1")
				t2 := TableOf(&OrderDetail{}, "t2")
				t3 := t1.Join(t2).On(t1.C("Id").EQ(t2.C("OrderId")))
				t4 := TableOf(&Item{}, "t4")
				t5 := t4.Join(t3).On(t2.C("ItemId").EQ(t4.C("Id")))
				return NewSelector[Order](db).Select(t4.AllColumns()).From(t5)
			}(),
			wantQuery: Query{
				SQL: "SELECT `t4`.* FROM (`item` AS `t4` JOIN (`order` AS `t1` JOIN `order_detail` AS `t2` ON `t1`.`id`=`t2`.`order_id`) ON `t2`.`item_id`=`t4`.`id`);",
			},
		},
		{
			name: "table join on Sum",
			s: func() QueryBuilder {
				t1 := TableOf(&Order{}, "t1")
				t2 := TableOf(&OrderDetail{}, "t2")
				t3 := t1.Join(t2).On(t1.C("Id").EQ(t2.C("OrderId")))
				t4 := TableOf(&Item{}, "t4")
				t5 := t4.Join(t3).On(t2.C("ItemId").EQ(t4.C("Id")))
				return NewSelector[Order](db).From(t5).Select(t4.Sum("Id").As("sum_id"), t4.Min("Id").As("min_id"), t4.Max("Id").As("max_id"), t4.Sum("Id").As("t4_sum_id"), t4.Count("Id").As("t4_cnt_id"))
			}(),
			wantQuery: Query{
				SQL: "SELECT SUM(`t4`.`id`) AS `sum_id`,MIN(`t4`.`id`) AS `min_id`,MAX(`t4`.`id`) AS `max_id`,SUM(`t4`.`id`) AS `t4_sum_id`,COUNT(`t4`.`id`) AS `t4_cnt_id` FROM (`item` AS `t4` JOIN (`order` AS `t1` JOIN `order_detail` AS `t2` ON `t1`.`id`=`t2`.`order_id`) ON `t2`.`item_id`=`t4`.`id`);",
			},
		},
		{
			name: "table join col",
			s: func() QueryBuilder {
				t1 := TableOf(&test.Order{}, "t1")
				t2 := TableOf(&test.OrderDetail{}, "t2")
				t3 := t1.Join(t2).On(t1.C("Id").EQ(t2.C("OrderId")))
				return NewSelector[test.Order](db).From(t3).Select(t1.Avg("UsingCol1").As("UsingCol1"))
			}(),
			wantQuery: Query{
				SQL: "SELECT AVG(`t1`.`using_col1`) AS `UsingCol1` FROM (`order` AS `t1` JOIN `order_detail` AS `t2` ON `t1`.`id`=`t2`.`order_id`);",
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			q, err := tc.s.Build()
			assert.Equal(t, tc.wantErr, err)
			if err != nil {
				return
			}
			assert.Equal(t, tc.wantQuery, q)
		})
	}
}

func TestSelector_Subquery(t *testing.T) {
	db := memoryDB()
	type Order struct {
		Id        int
		UsingCol1 string
		UsingCol2 string
	}

	type OrderDetail struct {
		OrderId   int
		ItemId    int
		UsingCol1 string
		UsingCol2 string
	}

	testCases := []struct {
		name      string
		s         QueryBuilder
		wantQuery Query
		wantErr   error
	}{
		// 子查詢
		{
			name: "from",
			s: func() QueryBuilder {
				sub := NewSelector[OrderDetail](db).AsSubquery("sub")
				return NewSelector[Order](db).Select(Raw("*")).From(sub)
			}(),
			wantQuery: Query{
				SQL: "SELECT * FROM (SELECT `order_id`,`item_id`,`using_col1`,`using_col2` FROM `order_detail`) AS `sub`;"},
		},
		{
			name: "from & where",
			s: func() QueryBuilder {
				o1 := TableOf(&OrderDetail{}, "o1")
				sub := NewSelector[OrderDetail](db).From(o1).Where(o1.C("OrderId").GT(18)).AsSubquery("sub")
				return NewSelector[Order](db).Select(Raw("*")).From(sub)
			}(),

			wantQuery: Query{
				SQL:  "SELECT * FROM (SELECT `order_id`,`item_id`,`using_col1`,`using_col2` FROM `order_detail` AS `o1` WHERE `o1`.`order_id`>?) AS `sub`;",
				Args: []any{18},
			},
		},
		{
			name: "in",
			s: func() QueryBuilder {
				o1 := TableOf(&Order{}, "o1")
				sub := NewSelector[OrderDetail](db).Select(C("OrderId")).AsSubquery("sub")
				return NewSelector[Order](db).Select(o1.C("Id")).From(o1).Where(o1.C("Id").In(sub))
			}(),
			wantQuery: Query{
				SQL: "SELECT `o1`.`id` FROM `order` AS `o1` WHERE `o1`.`id` IN (SELECT `order_id` FROM `order_detail`);"},
		},
		{
			name: "all",
			s: func() QueryBuilder {
				o1 := TableOf(&Order{}, "o1")
				sub := NewSelector[OrderDetail](db).Select(C("OrderId")).AsSubquery("sub")
				return NewSelector[Order](db).Select(o1.C("Id"), o1.C("UsingCol1"), o1.C("UsingCol2")).From(o1).Where(o1.C("Id").GT(All(sub)))
			}(),
			wantQuery: Query{
				SQL: "SELECT `o1`.`id`,`o1`.`using_col1`,`o1`.`using_col2` FROM `order` AS `o1` WHERE `o1`.`id`>ALL (SELECT `order_id` FROM `order_detail`);"},
		},
		{
			name: "some and any",
			s: func() QueryBuilder {
				o1 := TableOf(&Order{}, "o1")
				sub := NewSelector[OrderDetail](db).Select(C("OrderId")).AsSubquery("sub")
				return NewSelector[Order](db).From(o1).Where(o1.C("Id").GT(Some(sub)), o1.C("Id").LT(Any(sub)))
			}(),
			wantQuery: Query{
				SQL: "SELECT `id`,`using_col1`,`using_col2` FROM `order` AS `o1` WHERE (`o1`.`id`>SOME (SELECT `order_id` FROM `order_detail`)) AND (`o1`.`id`<ANY (SELECT `order_id` FROM `order_detail`));"},
		},
		{
			name: "exist",
			s: func() QueryBuilder {
				sub := NewSelector[OrderDetail](db).Select(C("OrderId")).AsSubquery("sub")
				return NewSelector[Order](db).Where(Exist(sub))
			}(),
			wantQuery: Query{
				SQL: "SELECT `id`,`using_col1`,`using_col2` FROM `order` WHERE EXIST (SELECT `order_id` FROM `order_detail`);"},
		},
		{
			name: "not exist",
			s: func() QueryBuilder {
				sub := NewSelector[OrderDetail](db).Select(C("OrderId")).AsSubquery("sub")
				return NewSelector[Order](db).Where(Not(Exist(sub)))
			}(),
			wantQuery: Query{
				SQL: "SELECT `id`,`using_col1`,`using_col2` FROM `order` WHERE NOT (EXIST (SELECT `order_id` FROM `order_detail`));"},
		},
		{
			name: "aggregate",
			s: func() QueryBuilder {
				sub := NewSelector[OrderDetail](db).Select(C("OrderId")).AsSubquery("sub")
				return NewSelector[Order](db).Select(Max("Id")).Where(Exist(sub))
			}(),
			wantQuery: Query{
				SQL: "SELECT MAX(`id`) FROM `order` WHERE EXIST (SELECT `order_id` FROM `order_detail`);"},
		},
		{
			name: "invalid column",
			s: func() QueryBuilder {
				sub := NewSelector[OrderDetail](db).Select(C("OrderId")).AsSubquery("sub")
				return NewSelector[Order](db).Select(Max("invalid")).Where(Exist(sub))
			}(),
			wantErr: errs.NewInvalidFieldError("invalid"),
		},
		// Join 與 Subquery 一起使用測試
		{
			name: "join & subquery",
			s: func() QueryBuilder {
				sub1 := NewSelector[Order](db).AsSubquery("sub1")
				sub := NewSelector[OrderDetail](db).AsSubquery("sub")
				return NewSelector[Order](db).Select(sub.C("OrderId")).From(sub1.Join(sub).On(sub1.C("Id").EQ(sub.C("OrderId")))).Where()
			}(),
			wantQuery: Query{
				SQL: "SELECT `sub`.`order_id` FROM ((SELECT `id`,`using_col1`,`using_col2` FROM `order`) AS `sub1` JOIN (SELECT `order_id`,`item_id`,`using_col1`,`using_col2` FROM `order_detail`) AS `sub` ON `sub1`.`id`=`sub`.`order_id`);"},
		},
		{
			name: "left join & subquery",
			s: func() QueryBuilder {
				sub1 := NewSelector[Order](db).AsSubquery("sub1")
				sub := NewSelector[OrderDetail](db).AsSubquery("sub")
				return NewSelector[Order](db).Select(sub.C("OrderId")).From(sub1.LeftJoin(sub).On(sub1.C("Id").EQ(sub.C("OrderId")))).Where()
			}(),
			wantQuery: Query{
				SQL: "SELECT `sub`.`order_id` FROM ((SELECT `id`,`using_col1`,`using_col2` FROM `order`) AS `sub1` LEFT JOIN (SELECT `order_id`,`item_id`,`using_col1`,`using_col2` FROM `order_detail`) AS `sub` ON `sub1`.`id`=`sub`.`order_id`);"},
		},
		{
			name: "right join & subquery",
			s: func() QueryBuilder {
				sub1 := NewSelector[Order](db).AsSubquery("sub1")
				sub := NewSelector[OrderDetail](db).AsSubquery("sub")
				return NewSelector[Order](db).Select(sub.C("OrderId")).From(sub1.RightJoin(sub).On(sub1.C("Id").EQ(sub.C("OrderId")))).Where()
			}(),
			wantQuery: Query{
				SQL: "SELECT `sub`.`order_id` FROM ((SELECT `id`,`using_col1`,`using_col2` FROM `order`) AS `sub1` RIGHT JOIN (SELECT `order_id`,`item_id`,`using_col1`,`using_col2` FROM `order_detail`) AS `sub` ON `sub1`.`id`=`sub`.`order_id`);"},
		},
		{
			name: "right join & subquery & using",
			s: func() QueryBuilder {
				sub1 := NewSelector[OrderDetail](db).AsSubquery("sub1")
				sub2 := NewSelector[OrderDetail](db).AsSubquery("sub2")
				return NewSelector[Order](db).Select(sub1.C("OrderId")).From(sub1.RightJoin(sub2).Using("Id")).Where()
			}(),
			wantQuery: Query{
				SQL: "SELECT `sub1`.`order_id` FROM ((SELECT `order_id`,`item_id`,`using_col1`,`using_col2` FROM `order_detail`) AS `sub1` RIGHT JOIN (SELECT `order_id`,`item_id`,`using_col1`,`using_col2` FROM `order_detail`) AS `sub2` USING (`id`));"},
		},
		{
			name: "join & subquery & using",
			s: func() QueryBuilder {
				sub1 := NewSelector[OrderDetail](db).AsSubquery("sub1")
				sub2 := NewSelector[OrderDetail](db).Select(sub1.C("OrderId")).From(sub1).AsSubquery("sub2")
				t1 := TableOf(&Order{}, "")
				return NewSelector[Order](db).Select(t1.C("Id")).From(sub2.Join(sub1).Using("Id")).Where()
			}(),
			wantQuery: Query{
				SQL: "SELECT `id` FROM ((SELECT `sub1`.`order_id` FROM (SELECT `order_id`,`item_id`,`using_col1`,`using_col2` FROM `order_detail`) AS `sub1`) AS `sub2` JOIN (SELECT `order_id`,`item_id`,`using_col1`,`using_col2` FROM `order_detail`) AS `sub1` USING (`id`));"},
		},
		{
			name: "invalid field",
			s: func() QueryBuilder {
				t1 := TableOf(&Order{}, "")
				sub := NewSelector[OrderDetail](db).AsSubquery("sub")
				return NewSelector[Order](db).Select(sub.C("Invalid")).From(t1.Join(sub).On(t1.C("Id").EQ(sub.C("OrderId")))).Where()
			}(),
			wantErr: errs.NewInvalidFieldError("Invalid"),
		},
		{
			name: "invalid field in predicates",
			s: func() QueryBuilder {
				t1 := TableOf(&Order{}, "")
				sub := NewSelector[OrderDetail](db).AsSubquery("sub")
				return NewSelector[Order](db).Select(sub.C("OrderId")).From(t1.Join(sub).On(t1.C("Id").EQ(sub.C("Invalid")))).Where()
			}(),
			wantErr: errs.NewInvalidFieldError("Invalid"),
		},
		{
			name: "invalid field in predicates with columns",
			s: func() QueryBuilder {
				t1 := TableOf(&Order{}, "")
				sub := NewSelector[OrderDetail](db).Select(C("OrderId")).AsSubquery("sub")
				return NewSelector[Order](db).Select(sub.C("Invalid")).From(t1.Join(sub).On(t1.C("Id").EQ(sub.C("OrderId")))).Where()
			}(),
			wantErr: errs.NewInvalidFieldError("Invalid"),
		},
		{
			name: "invalid field in aggregate function",
			s: func() QueryBuilder {
				t1 := TableOf(&Order{}, "")
				sub := NewSelector[OrderDetail](db).AsSubquery("sub")
				return NewSelector[Order](db).Select(Max("Invalid")).From(t1.Join(sub).On(t1.C("Id").EQ(sub.C("OrderId")))).Where()
			}(),
			wantErr: errs.NewInvalidFieldError("Invalid"),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			q, err := tc.s.Build()
			assert.Equal(t, tc.wantErr, err)
			if err != nil {
				return
			}
			assert.Equal(t, tc.wantQuery, q)
		})
	}
}
