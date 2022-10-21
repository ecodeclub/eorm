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

package model

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/gotomicro/eorm/internal/errs"

	"github.com/stretchr/testify/assert"
)

func TestTagMetaRegistry(t *testing.T) {

	testCases := []struct {
		name     string
		wantMeta *TableMeta
		wantErr  error
		input    interface{}
	}{
		{
			// 普通
			name: "normal model",
			wantMeta: &TableMeta{
				TableName: "test_model",
				Columns: []*ColumnMeta{
					{
						ColumnName:      "id",
						FieldName:       "Id",
						Typ:             reflect.TypeOf(int64(0)),
						IsPrimaryKey:    true,
						IsAutoIncrement: true,
						FieldIndexes:    []int{0},
					},
					{
						ColumnName:   "first_name",
						FieldName:    "FirstName",
						Typ:          reflect.TypeOf(""),
						Offset:       8,
						FieldIndexes: []int{1},
					},
					{
						ColumnName:   "age",
						FieldName:    "Age",
						Typ:          reflect.TypeOf(int8(0)),
						Offset:       24,
						FieldIndexes: []int{2},
					},
					{
						ColumnName:   "last_name",
						FieldName:    "LastName",
						Typ:          reflect.TypeOf((*string)(nil)),
						Offset:       32,
						FieldIndexes: []int{3},
					},
				},
				FieldMap: map[string]*ColumnMeta{
					"Id": {
						ColumnName:      "id",
						FieldName:       "Id",
						Typ:             reflect.TypeOf(int64(0)),
						IsPrimaryKey:    true,
						IsAutoIncrement: true,
						FieldIndexes:    []int{0},
					},
					"FirstName": {
						ColumnName:   "first_name",
						FieldName:    "FirstName",
						Typ:          reflect.TypeOf(""),
						Offset:       8,
						FieldIndexes: []int{1},
					},
					"Age": {
						ColumnName:   "age",
						FieldName:    "Age",
						Typ:          reflect.TypeOf(int8(0)),
						Offset:       24,
						FieldIndexes: []int{2},
					},
					"LastName": {
						ColumnName:   "last_name",
						FieldName:    "LastName",
						Typ:          reflect.TypeOf((*string)(nil)),
						Offset:       32,
						FieldIndexes: []int{3},
					},
				},
				ColumnMap: map[string]*ColumnMeta{
					"id": {
						ColumnName:      "id",
						FieldName:       "Id",
						Typ:             reflect.TypeOf(int64(0)),
						IsPrimaryKey:    true,
						IsAutoIncrement: true,
						FieldIndexes:    []int{0},
					},
					"first_name": {
						ColumnName:   "first_name",
						FieldName:    "FirstName",
						Typ:          reflect.TypeOf(""),
						Offset:       8,
						FieldIndexes: []int{1},
					},
					"age": {
						ColumnName:   "age",
						FieldName:    "Age",
						Typ:          reflect.TypeOf(int8(0)),
						Offset:       24,
						FieldIndexes: []int{2},
					},
					"last_name": {
						ColumnName:   "last_name",
						FieldName:    "LastName",
						Typ:          reflect.TypeOf((*string)(nil)),
						Offset:       32,
						FieldIndexes: []int{3},
					},
				},
				Typ: reflect.TypeOf(&TestModel{}),
			},
			input: &TestModel{},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			registry := &tagMetaRegistry{}
			meta, err := registry.Register(tc.input)
			assert.Equal(t, tc.wantErr, err)
			if err != nil {
				return
			}
			assert.Equal(t, tc.wantMeta, meta)
		})
	}
}
func TestTagMetaRegistry_Combination(t *testing.T) {

	testCases := []struct {
		name     string
		wantMeta *TableMeta
		wantErr  error
		input    interface{}
	}{
		// 普通组合
		{
			name: "普通组合",
			wantMeta: &TableMeta{
				TableName: "test_combined_model",
				Columns: []*ColumnMeta{
					{
						ColumnName:      "create_time",
						FieldName:       "CreateTime",
						Typ:             reflect.TypeOf(uint64(0)),
						IsPrimaryKey:    false,
						IsAutoIncrement: false,
						Offset:          0,
						FieldIndexes:    []int{0, 0},
					}, {
						ColumnName:      "update_time",
						FieldName:       "UpdateTime",
						Typ:             reflect.TypeOf(uint64(0)),
						IsPrimaryKey:    false,
						IsAutoIncrement: false,
						Offset:          8,
						FieldIndexes:    []int{0, 1},
					},
					{
						ColumnName:      "id",
						FieldName:       "Id",
						Typ:             reflect.TypeOf(int64(0)),
						IsPrimaryKey:    true,
						IsAutoIncrement: true,
						Offset:          16,
						FieldIndexes:    []int{1},
					},
					{
						ColumnName:   "first_name",
						FieldName:    "FirstName",
						Typ:          reflect.TypeOf(""),
						Offset:       24,
						FieldIndexes: []int{2},
					},
					{
						ColumnName:   "age",
						FieldName:    "Age",
						Typ:          reflect.TypeOf(int8(0)),
						Offset:       40,
						FieldIndexes: []int{3},
					},
					{
						ColumnName:   "last_name",
						FieldName:    "LastName",
						Typ:          reflect.TypeOf((*string)(nil)),
						Offset:       48,
						FieldIndexes: []int{4},
					},
				},
				FieldMap: map[string]*ColumnMeta{
					"CreateTime": {
						ColumnName:      "create_time",
						FieldName:       "CreateTime",
						Typ:             reflect.TypeOf(uint64(0)),
						IsPrimaryKey:    false,
						IsAutoIncrement: false,
						Offset:          0,
						FieldIndexes:    []int{0, 0},
					},
					"UpdateTime": {
						ColumnName:      "update_time",
						FieldName:       "UpdateTime",
						Typ:             reflect.TypeOf(uint64(0)),
						IsPrimaryKey:    false,
						IsAutoIncrement: false,
						Offset:          8,
						FieldIndexes:    []int{0, 1},
					},
					"Id": {
						ColumnName:      "id",
						FieldName:       "Id",
						Typ:             reflect.TypeOf(int64(0)),
						IsPrimaryKey:    true,
						IsAutoIncrement: true,
						Offset:          16,
						FieldIndexes:    []int{1},
					},
					"FirstName": {
						ColumnName:   "first_name",
						FieldName:    "FirstName",
						Typ:          reflect.TypeOf(""),
						Offset:       24,
						FieldIndexes: []int{2},
					},
					"Age": {
						ColumnName:   "age",
						FieldName:    "Age",
						Typ:          reflect.TypeOf(int8(0)),
						Offset:       40,
						FieldIndexes: []int{3},
					},
					"LastName": {
						ColumnName:   "last_name",
						FieldName:    "LastName",
						Typ:          reflect.TypeOf((*string)(nil)),
						Offset:       48,
						FieldIndexes: []int{4},
					},
				},
				ColumnMap: map[string]*ColumnMeta{
					"create_time": {
						ColumnName:      "create_time",
						FieldName:       "CreateTime",
						Typ:             reflect.TypeOf(uint64(0)),
						IsPrimaryKey:    false,
						IsAutoIncrement: false,
						Offset:          0,
						FieldIndexes:    []int{0, 0},
					},
					"update_time": {
						ColumnName:      "update_time",
						FieldName:       "UpdateTime",
						Typ:             reflect.TypeOf(uint64(0)),
						IsPrimaryKey:    false,
						IsAutoIncrement: false,
						Offset:          8,
						FieldIndexes:    []int{0, 1},
					},
					"id": {
						ColumnName:      "id",
						FieldName:       "Id",
						Typ:             reflect.TypeOf(int64(0)),
						IsPrimaryKey:    true,
						IsAutoIncrement: true,
						Offset:          16,
						FieldIndexes:    []int{1},
					},
					"first_name": {
						ColumnName:   "first_name",
						FieldName:    "FirstName",
						Typ:          reflect.TypeOf(""),
						Offset:       24,
						FieldIndexes: []int{2},
					},
					"age": {
						ColumnName:   "age",
						FieldName:    "Age",
						Typ:          reflect.TypeOf(int8(0)),
						Offset:       40,
						FieldIndexes: []int{3},
					},
					"last_name": {
						ColumnName:   "last_name",
						FieldName:    "LastName",
						Typ:          reflect.TypeOf((*string)(nil)),
						Offset:       48,
						FieldIndexes: []int{4},
					},
				},
				Typ: reflect.TypeOf(&TestCombinedModel{}),
			},
			input: &TestCombinedModel{},
		},
		// 指针组合
		{
			name:    "指针组合",
			input:   &TestCombinedModelPtr{},
			wantErr: errs.ErrCombinationIsNotStruct,
		},
		// 忽略组合
		{
			name: "忽略组合",
			wantMeta: &TableMeta{
				TableName: "test_combined_model_ignore",
				Columns: []*ColumnMeta{
					{
						ColumnName:      "id",
						FieldName:       "Id",
						Typ:             reflect.TypeOf(int64(0)),
						IsPrimaryKey:    true,
						IsAutoIncrement: true,
						Offset:          16,
						FieldIndexes:    []int{1},
					},
					{
						ColumnName:   "first_name",
						FieldName:    "FirstName",
						Typ:          reflect.TypeOf(""),
						Offset:       24,
						FieldIndexes: []int{2},
					},
					{
						ColumnName:   "age",
						FieldName:    "Age",
						Typ:          reflect.TypeOf(int8(0)),
						Offset:       40,
						FieldIndexes: []int{3},
					},
					{
						ColumnName:   "last_name",
						FieldName:    "LastName",
						Typ:          reflect.TypeOf((*string)(nil)),
						Offset:       48,
						FieldIndexes: []int{4},
					},
				},
				FieldMap: map[string]*ColumnMeta{
					"Id": {
						ColumnName:      "id",
						FieldName:       "Id",
						Typ:             reflect.TypeOf(int64(0)),
						IsPrimaryKey:    true,
						IsAutoIncrement: true,
						Offset:          16,
						FieldIndexes:    []int{1},
					},
					"FirstName": {
						ColumnName:   "first_name",
						FieldName:    "FirstName",
						Typ:          reflect.TypeOf(""),
						Offset:       24,
						FieldIndexes: []int{2},
					},
					"Age": {
						ColumnName:   "age",
						FieldName:    "Age",
						Typ:          reflect.TypeOf(int8(0)),
						Offset:       40,
						FieldIndexes: []int{3},
					},
					"LastName": {
						ColumnName:   "last_name",
						FieldName:    "LastName",
						Typ:          reflect.TypeOf((*string)(nil)),
						Offset:       48,
						FieldIndexes: []int{4},
					},
				},
				ColumnMap: map[string]*ColumnMeta{
					"id": {
						ColumnName:      "id",
						FieldName:       "Id",
						Typ:             reflect.TypeOf(int64(0)),
						IsPrimaryKey:    true,
						IsAutoIncrement: true,
						Offset:          16,
						FieldIndexes:    []int{1},
					},
					"first_name": {
						ColumnName:   "first_name",
						FieldName:    "FirstName",
						Typ:          reflect.TypeOf(""),
						Offset:       24,
						FieldIndexes: []int{2},
					},
					"age": {
						ColumnName:   "age",
						FieldName:    "Age",
						Typ:          reflect.TypeOf(int8(0)),
						Offset:       40,
						FieldIndexes: []int{3},
					},
					"last_name": {
						ColumnName:   "last_name",
						FieldName:    "LastName",
						Typ:          reflect.TypeOf((*string)(nil)),
						Offset:       48,
						FieldIndexes: []int{4},
					},
				},
				Typ: reflect.TypeOf(&TestCombinedModelIgnore{}),
			},
			input: &TestCombinedModelIgnore{},
		},
		// 多重组合
		{
			name: "多重组合",
			wantMeta: &TableMeta{
				TableName: "test_combined_model_multi",
				Columns: []*ColumnMeta{
					{
						ColumnName:      "create_time",
						FieldName:       "CreateTime",
						Typ:             reflect.TypeOf(uint64(0)),
						IsPrimaryKey:    false,
						IsAutoIncrement: false,
						Offset:          0,
						FieldIndexes:    []int{0, 0},
					},
					{
						ColumnName:      "update_time",
						FieldName:       "UpdateTime",
						Typ:             reflect.TypeOf(uint64(0)),
						IsPrimaryKey:    false,
						IsAutoIncrement: false,
						Offset:          8,
						FieldIndexes:    []int{0, 1},
					},
					{
						ColumnName:      "id",
						FieldName:       "Id",
						Typ:             reflect.TypeOf(int64(0)),
						IsPrimaryKey:    true,
						IsAutoIncrement: true,
						Offset:          16,
						FieldIndexes:    []int{1},
					},
					{
						ColumnName:   "first_name",
						FieldName:    "FirstName",
						Typ:          reflect.TypeOf(""),
						Offset:       24,
						FieldIndexes: []int{2},
					},
					{
						ColumnName:   "age",
						FieldName:    "Age",
						Typ:          reflect.TypeOf(int8(0)),
						Offset:       40,
						FieldIndexes: []int{3},
					},
					{
						ColumnName:   "last_name",
						FieldName:    "LastName",
						Typ:          reflect.TypeOf((*string)(nil)),
						Offset:       48,
						FieldIndexes: []int{4},
					},
					{
						ColumnName:   "phone",
						FieldName:    "Phone",
						Typ:          reflect.TypeOf(""),
						Offset:       56,
						FieldIndexes: []int{5, 0},
					},
					{
						ColumnName:   "address",
						FieldName:    "Address",
						Typ:          reflect.TypeOf(""),
						Offset:       72,
						FieldIndexes: []int{5, 1},
					},
				},
				FieldMap: map[string]*ColumnMeta{
					"CreateTime": {
						ColumnName:      "create_time",
						FieldName:       "CreateTime",
						Typ:             reflect.TypeOf(uint64(0)),
						IsPrimaryKey:    false,
						IsAutoIncrement: false,
						Offset:          0,
						FieldIndexes:    []int{0, 0},
					},
					"UpdateTime": {
						ColumnName:      "update_time",
						FieldName:       "UpdateTime",
						Typ:             reflect.TypeOf(uint64(0)),
						IsPrimaryKey:    false,
						IsAutoIncrement: false,
						Offset:          8,
						FieldIndexes:    []int{0, 1},
					},
					"Id": {
						ColumnName:      "id",
						FieldName:       "Id",
						Typ:             reflect.TypeOf(int64(0)),
						IsPrimaryKey:    true,
						IsAutoIncrement: true,
						Offset:          16,
						FieldIndexes:    []int{1},
					},
					"FirstName": {
						ColumnName:   "first_name",
						FieldName:    "FirstName",
						Typ:          reflect.TypeOf(""),
						Offset:       24,
						FieldIndexes: []int{2},
					},
					"Age": {
						ColumnName:   "age",
						FieldName:    "Age",
						Typ:          reflect.TypeOf(int8(0)),
						Offset:       40,
						FieldIndexes: []int{3},
					},
					"LastName": {
						ColumnName:   "last_name",
						FieldName:    "LastName",
						Typ:          reflect.TypeOf((*string)(nil)),
						Offset:       48,
						FieldIndexes: []int{4},
					},
					"Phone": {
						ColumnName:   "phone",
						FieldName:    "Phone",
						Typ:          reflect.TypeOf(""),
						Offset:       56,
						FieldIndexes: []int{5, 0},
					},
					"Address": {
						ColumnName:   "address",
						FieldName:    "Address",
						Typ:          reflect.TypeOf(""),
						Offset:       72,
						FieldIndexes: []int{5, 1},
					},
				},
				ColumnMap: map[string]*ColumnMeta{
					"create_time": {
						ColumnName:      "create_time",
						FieldName:       "CreateTime",
						Typ:             reflect.TypeOf(uint64(0)),
						IsPrimaryKey:    false,
						IsAutoIncrement: false,
						Offset:          0,
						FieldIndexes:    []int{0, 0},
					},
					"update_time": {
						ColumnName:      "update_time",
						FieldName:       "UpdateTime",
						Typ:             reflect.TypeOf(uint64(0)),
						IsPrimaryKey:    false,
						IsAutoIncrement: false,
						Offset:          8,
						FieldIndexes:    []int{0, 1},
					},
					"id": {
						ColumnName:      "id",
						FieldName:       "Id",
						Typ:             reflect.TypeOf(int64(0)),
						IsPrimaryKey:    true,
						IsAutoIncrement: true,
						Offset:          16,
						FieldIndexes:    []int{1},
					},
					"first_name": {
						ColumnName:   "first_name",
						FieldName:    "FirstName",
						Typ:          reflect.TypeOf(""),
						Offset:       24,
						FieldIndexes: []int{2},
					},
					"age": {
						ColumnName:   "age",
						FieldName:    "Age",
						Typ:          reflect.TypeOf(int8(0)),
						Offset:       40,
						FieldIndexes: []int{3},
					},
					"last_name": {
						ColumnName:   "last_name",
						FieldName:    "LastName",
						Typ:          reflect.TypeOf((*string)(nil)),
						Offset:       48,
						FieldIndexes: []int{4},
					},
					"phone": {
						ColumnName:   "phone",
						FieldName:    "Phone",
						Typ:          reflect.TypeOf(""),
						Offset:       56,
						FieldIndexes: []int{5, 0},
					},
					"address": {
						ColumnName:   "address",
						FieldName:    "Address",
						Typ:          reflect.TypeOf(""),
						Offset:       72,
						FieldIndexes: []int{5, 1},
					},
				},
				Typ: reflect.TypeOf(&TestCombinedModelMulti{}),
			},
			input: &TestCombinedModelMulti{},
		},
		// 嵌套组合
		{
			name: "嵌套组合",
			wantMeta: &TableMeta{
				TableName: "test_combined_model_nested",
				Columns: []*ColumnMeta{
					{
						ColumnName:      "create_time",
						FieldName:       "CreateTime",
						Typ:             reflect.TypeOf(uint64(0)),
						IsPrimaryKey:    false,
						IsAutoIncrement: false,
						Offset:          0,
						FieldIndexes:    []int{0, 0, 0},
					}, {
						ColumnName:      "update_time",
						FieldName:       "UpdateTime",
						Typ:             reflect.TypeOf(uint64(0)),
						IsPrimaryKey:    false,
						IsAutoIncrement: false,
						Offset:          8,
						FieldIndexes:    []int{0, 0, 1},
					},
					{
						ColumnName:      "id",
						FieldName:       "Id",
						Typ:             reflect.TypeOf(int64(0)),
						IsPrimaryKey:    true,
						IsAutoIncrement: true,
						Offset:          16,
						FieldIndexes:    []int{0, 1},
					},
					{
						ColumnName:   "first_name",
						FieldName:    "FirstName",
						Typ:          reflect.TypeOf(""),
						Offset:       24,
						FieldIndexes: []int{0, 2},
					},
					{
						ColumnName:   "age",
						FieldName:    "Age",
						Typ:          reflect.TypeOf(int8(0)),
						Offset:       40,
						FieldIndexes: []int{0, 3},
					},
					{
						ColumnName:   "last_name",
						FieldName:    "LastName",
						Typ:          reflect.TypeOf((*string)(nil)),
						Offset:       48,
						FieldIndexes: []int{0, 4},
					},
				},
				FieldMap: map[string]*ColumnMeta{
					"CreateTime": {
						ColumnName:      "create_time",
						FieldName:       "CreateTime",
						Typ:             reflect.TypeOf(uint64(0)),
						IsPrimaryKey:    false,
						IsAutoIncrement: false,
						Offset:          0,
						FieldIndexes:    []int{0, 0, 0},
					},
					"UpdateTime": {
						ColumnName:      "update_time",
						FieldName:       "UpdateTime",
						Typ:             reflect.TypeOf(uint64(0)),
						IsPrimaryKey:    false,
						IsAutoIncrement: false,
						Offset:          8,
						FieldIndexes:    []int{0, 0, 1},
					},
					"Id": {
						ColumnName:      "id",
						FieldName:       "Id",
						Typ:             reflect.TypeOf(int64(0)),
						IsPrimaryKey:    true,
						IsAutoIncrement: true,
						Offset:          16,
						FieldIndexes:    []int{0, 1},
					},
					"FirstName": {
						ColumnName:   "first_name",
						FieldName:    "FirstName",
						Typ:          reflect.TypeOf(""),
						Offset:       24,
						FieldIndexes: []int{0, 2},
					},
					"Age": {
						ColumnName:   "age",
						FieldName:    "Age",
						Typ:          reflect.TypeOf(int8(0)),
						Offset:       40,
						FieldIndexes: []int{0, 3},
					},
					"LastName": {
						ColumnName:   "last_name",
						FieldName:    "LastName",
						Typ:          reflect.TypeOf((*string)(nil)),
						Offset:       48,
						FieldIndexes: []int{0, 4},
					},
				},
				ColumnMap: map[string]*ColumnMeta{
					"create_time": {
						ColumnName:      "create_time",
						FieldName:       "CreateTime",
						Typ:             reflect.TypeOf(uint64(0)),
						IsPrimaryKey:    false,
						IsAutoIncrement: false,
						Offset:          0,
						FieldIndexes:    []int{0, 0, 0},
					},
					"update_time": {
						ColumnName:      "update_time",
						FieldName:       "UpdateTime",
						Typ:             reflect.TypeOf(uint64(0)),
						IsPrimaryKey:    false,
						IsAutoIncrement: false,
						Offset:          8,
						FieldIndexes:    []int{0, 0, 1},
					},
					"id": {
						ColumnName:      "id",
						FieldName:       "Id",
						Typ:             reflect.TypeOf(int64(0)),
						IsPrimaryKey:    true,
						IsAutoIncrement: true,
						Offset:          16,
						FieldIndexes:    []int{0, 1},
					},
					"first_name": {
						ColumnName:   "first_name",
						FieldName:    "FirstName",
						Typ:          reflect.TypeOf(""),
						Offset:       24,
						FieldIndexes: []int{0, 2},
					},
					"age": {
						ColumnName:   "age",
						FieldName:    "Age",
						Typ:          reflect.TypeOf(int8(0)),
						Offset:       40,
						FieldIndexes: []int{0, 3},
					},
					"last_name": {
						ColumnName:   "last_name",
						FieldName:    "LastName",
						Typ:          reflect.TypeOf((*string)(nil)),
						Offset:       48,
						FieldIndexes: []int{0, 4},
					},
				},
				Typ: reflect.TypeOf(&TestCombinedModelNested{}),
			},
			input: &TestCombinedModelNested{},
		},
		// 组合字段冲突
		{
			name:    "组合字段冲突",
			input:   &TestCombinedModelConflict{},
			wantErr: errs.NewFieldConflictError("TestCombinedModelConflict.Id"),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			registry := &tagMetaRegistry{}
			meta, err := registry.Register(tc.input)
			assert.Equal(t, tc.wantErr, err)
			if err != nil {
				return
			}
			assert.Equal(t, tc.wantMeta, meta)
			//assert.Equal(t, tc.wantMeta.Columns, meta.Columns)
			//assert.Equal(t, tc.wantMeta.FieldMap, meta.FieldMap)
			//assert.Equal(t, tc.wantMeta.ColumnMap, meta.ColumnMap)
			//assert.Equal(t, tc.wantMeta.Typ, meta.Typ)
			//assert.Equal(t, tc.wantMeta.TableName, meta.TableName)
			//for i := 0; i < len(meta.Columns); i++ {
			//	assert.Equal(t, tc.wantMeta.Columns[i], meta.Columns[i])
			//	//assert.Equal(t, tc.wantMeta.Columns[i].Typ, meta.Columns[i].Typ)
			//	//assert.Equal(t, tc.wantMeta.Columns[i].Offset, meta.Columns[i].Offset)
			//}
		})
	}
}

func TestIgnoreFieldsOption(t *testing.T) {
	tm := &TestIgnoreModel{}
	registry := &tagMetaRegistry{}
	meta, err := registry.Register(tm, IgnoreFieldsOption("Id", "FirstName"))
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, 1, len(meta.Columns))
	assert.Equal(t, 1, len(meta.FieldMap))
	assert.Equal(t, reflect.TypeOf(tm), meta.Typ)
	assert.Equal(t, "test_ignore_model", meta.TableName)

	_, hasId := meta.FieldMap["Id"]
	assert.False(t, hasId)

	_, hasFirstName := meta.FieldMap["FirstName"]
	assert.False(t, hasFirstName)

	_, hasAge := meta.FieldMap["Age"]
	assert.False(t, hasAge)

	_, hasLastName := meta.FieldMap["LastName"]
	assert.True(t, hasLastName)
}

type TestIgnoreModel struct {
	Id        int64 `eorm:"auto_increment,primary_key,-"`
	FirstName string
	Age       int8 `eorm:"-"`
	LastName  string
}

func ExampleMetaRegistry_Get() {
	tm := &TestModel{}
	registry := &tagMetaRegistry{}
	meta, _ := registry.Get(tm)
	fmt.Printf("table name: %v\n", meta.TableName)

	// Output:
	// table name: test_model
}

func ExampleMetaRegistry_Register() {
	// case1 without TableMetaOption
	tm := &TestModel{}
	registry := &tagMetaRegistry{}
	meta, _ := registry.Register(tm)
	fmt.Printf(`
case1：
	table name：%s
	column names：%s,%s,%s,%s
`, meta.TableName, meta.Columns[0].ColumnName, meta.Columns[1].ColumnName, meta.Columns[2].ColumnName, meta.Columns[3].ColumnName)

	// case2 use Tag to ignore field
	tim := &TestIgnoreModel{}
	registry = &tagMetaRegistry{}
	meta, _ = registry.Register(tim)
	fmt.Printf(`
case2：
	table name：%s
	column names：%s,%s
`, meta.TableName, meta.Columns[0].ColumnName, meta.Columns[1].ColumnName)

	// case3 use IgnoreFieldOption to ignore field
	tim = &TestIgnoreModel{}
	registry = &tagMetaRegistry{}
	meta, _ = registry.Register(tim, IgnoreFieldsOption("FirstName"))
	fmt.Printf(`
case3：
	table name：%s
	column names：%s
`, meta.TableName, meta.Columns[0].ColumnName)

	// Output:
	// case1：
	// 	table name：test_model
	// 	column names：id,first_name,age,last_name
	//
	// case2：
	// 	table name：test_ignore_model
	// 	column names：first_name,last_name
	//
	// case3：
	// 	table name：test_ignore_model
	// 	column names：last_name
}

type TestModel struct {
	Id        int64 `eorm:"auto_increment,primary_key"`
	FirstName string
	Age       int8
	LastName  *string
}

type BaseEntity struct {
	CreateTime uint64
	UpdateTime uint64
}

type BaseEntity2 struct {
	Id         int64 `eorm:"auto_increment,primary_key"`
	CreateTime uint64
	UpdateTime uint64
}

type Contact struct {
	Phone   string
	Address string
}

type TestCombinedModel struct {
	BaseEntity
	Id        int64 `eorm:"auto_increment,primary_key"`
	FirstName string
	Age       int8
	LastName  *string
}

type TestCombinedModelPtr struct {
	*BaseEntity
	Id        int64 `eorm:"auto_increment,primary_key"`
	FirstName string
	Age       int8
	LastName  *string
}

type TestCombinedModelIgnore struct {
	BaseEntity `eorm:"-"`
	Id         int64 `eorm:"auto_increment,primary_key"`
	FirstName  string
	Age        int8
	LastName   *string
}

type TestCombinedModelMulti struct {
	BaseEntity
	Id        int64 `eorm:"auto_increment,primary_key"`
	FirstName string
	Age       int8
	LastName  *string
	Contact
}

type TestCombinedModelNested struct {
	TestCombinedModel
}

type TestCombinedModelConflict struct {
	BaseEntity2
	Id        int64 `eorm:"auto_increment,primary_key"`
	FirstName string
	Age       int8
	LastName  *string
}
