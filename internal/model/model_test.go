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

package model

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/ecodeclub/eorm/internal/errs"

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
			wantMeta: tableMetaBuilder{
				TableName: "test_model",
				Columns: []*ColumnMeta{
					{
						ColumnName:   "id",
						FieldName:    "Id",
						Typ:          reflect.TypeOf(int64(0)),
						IsPrimaryKey: true,
						FieldIndexes: []int{0},
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
				Typ: reflect.TypeOf(&TestModel{}),
			}.build(),
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
			wantMeta: tableMetaBuilder{
				TableName: "test_combined_model",
				Columns: []*ColumnMeta{
					{
						ColumnName:   "create_time",
						FieldName:    "CreateTime",
						Typ:          reflect.TypeOf(uint64(0)),
						IsPrimaryKey: false,
						Offset:       0,
						FieldIndexes: []int{0, 0},
					}, {
						ColumnName:   "update_time",
						FieldName:    "UpdateTime",
						Typ:          reflect.TypeOf(uint64(0)),
						IsPrimaryKey: false,
						Offset:       8,
						FieldIndexes: []int{0, 1},
					},
					{
						ColumnName:   "id",
						FieldName:    "Id",
						Typ:          reflect.TypeOf(int64(0)),
						IsPrimaryKey: true,
						Offset:       16,
						FieldIndexes: []int{1},
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
				Typ: reflect.TypeOf(&TestCombinedModel{}),
			}.build(),
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
			wantMeta: tableMetaBuilder{
				TableName: "test_combined_model_ignore",
				Columns: []*ColumnMeta{
					{
						ColumnName:   "id",
						FieldName:    "Id",
						Typ:          reflect.TypeOf(int64(0)),
						IsPrimaryKey: true,
						Offset:       16,
						FieldIndexes: []int{1},
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
				Typ: reflect.TypeOf(&TestCombinedModelIgnore{}),
			}.build(),
			input: &TestCombinedModelIgnore{},
		},
		// 多重组合
		{
			name: "多重组合",
			wantMeta: tableMetaBuilder{
				TableName: "test_combined_model_multi",
				Columns: []*ColumnMeta{
					{
						ColumnName:   "create_time",
						FieldName:    "CreateTime",
						Typ:          reflect.TypeOf(uint64(0)),
						IsPrimaryKey: false,
						Offset:       0,
						FieldIndexes: []int{0, 0},
					},
					{
						ColumnName:   "update_time",
						FieldName:    "UpdateTime",
						Typ:          reflect.TypeOf(uint64(0)),
						IsPrimaryKey: false,
						Offset:       8,
						FieldIndexes: []int{0, 1},
					},
					{
						ColumnName:   "id",
						FieldName:    "Id",
						Typ:          reflect.TypeOf(int64(0)),
						IsPrimaryKey: true,
						Offset:       16,
						FieldIndexes: []int{1},
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
				Typ: reflect.TypeOf(&TestCombinedModelMulti{}),
			}.build(),
			input: &TestCombinedModelMulti{},
		},
		// 嵌套组合
		{
			name: "嵌套组合",
			wantMeta: tableMetaBuilder{
				TableName: "test_combined_model_nested",
				Columns: []*ColumnMeta{
					{
						ColumnName:   "create_time",
						FieldName:    "CreateTime",
						Typ:          reflect.TypeOf(uint64(0)),
						IsPrimaryKey: false,
						Offset:       0,
						FieldIndexes: []int{0, 0, 0},
					}, {
						ColumnName:   "update_time",
						FieldName:    "UpdateTime",
						Typ:          reflect.TypeOf(uint64(0)),
						IsPrimaryKey: false,
						Offset:       8,
						FieldIndexes: []int{0, 0, 1},
					},
					{
						ColumnName:   "id",
						FieldName:    "Id",
						Typ:          reflect.TypeOf(int64(0)),
						IsPrimaryKey: true,
						Offset:       16,
						FieldIndexes: []int{0, 1},
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
				Typ: reflect.TypeOf(&TestCombinedModelNested{}),
			}.build(),
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

	//	// case4 use IgnoreFieldOption to ignore field and sharding key
	//	tim = &TestIgnoreModel{}
	//	registry = &tagMetaRegistry{}
	//	meta, _ = registry.Register(tim, IgnoreFieldsOption("FirstName"),
	//		WithShardingKey("Id"))
	//	fmt.Printf(`
	//case4：
	//	table name：%s
	//	column names：%s
	//	sharding key name：%s
	//`, meta.TableName, meta.Columns[0].ColumnName, meta.ShardingKey)
	//
	//	// case5 use IgnoreFieldOption to ignore field and db ShardingFunc
	//	tim = &TestIgnoreModel{}
	//	registry = &tagMetaRegistry{}
	//	meta, _ = registry.Register(tim, IgnoreFieldsOption("FirstName"),
	//		WithDBShardingFunc(func(skVal any) (string, error) {
	//			db := skVal.(int) / 100
	//			return fmt.Sprintf("order_db_%d", db), nil
	//		}))
	//	dbName, _ := meta.DBShardingFunc(123)
	//	fmt.Printf(`
	//case5：
	//	table name：%s
	//	column names：%s
	//	db sharding name：%s
	//`, meta.TableName, meta.Columns[0].ColumnName, dbName)
	//
	//	// case6 use IgnoreFieldOption to ignore field and sharding key and table ShardingFunc
	//	tim = &TestIgnoreModel{}
	//	registry = &tagMetaRegistry{}
	//	meta, _ = registry.Register(tim, IgnoreFieldsOption("FirstName"),
	//		WithTableShardingFunc(func(skVal any) (string, error) {
	//			tbl := skVal.(int) % 10
	//			return fmt.Sprintf("order_tab_%d", tbl), nil
	//		}))
	//	tbName, _ := meta.TableShardingFunc(123)
	//	fmt.Printf(`
	//case6：
	//	table name：%s
	//	column names：%s
	//	table sharding name：%s
	//`, meta.TableName, meta.Columns[0].ColumnName, tbName)

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
	//	table name：test_ignore_model
	//	column names：last_name
}

type tableMetaBuilder struct {
	TableName string
	Columns   []*ColumnMeta
	Typ       reflect.Type
}

func (t tableMetaBuilder) build() *TableMeta {
	res := &TableMeta{
		TableName: t.TableName,
		Columns:   t.Columns,
		Typ:       t.Typ,
	}
	n := len(t.Columns)
	fieldMap := make(map[string]*ColumnMeta, n)
	columnMap := make(map[string]*ColumnMeta, n)
	for _, columnMeta := range t.Columns {
		fieldMap[columnMeta.FieldName] = columnMeta
		columnMap[columnMeta.ColumnName] = columnMeta
	}
	res.FieldMap = fieldMap
	res.ColumnMap = columnMap
	return res
}

type TestModel struct {
	Id        int64 `eorm:"primary_key"`
	FirstName string
	Age       int8
	LastName  *string
}

type BaseEntity struct {
	CreateTime uint64
	UpdateTime uint64
}

type BaseEntity2 struct {
	Id         int64 `eorm:"primary_key"`
	CreateTime uint64
	UpdateTime uint64
}

type Contact struct {
	Phone   string
	Address string
}

type TestCombinedModel struct {
	BaseEntity
	Id        int64 `eorm:"primary_key"`
	FirstName string
	Age       int8
	LastName  *string
}

type TestCombinedModelPtr struct {
	*BaseEntity
	Id        int64 `eorm:"primary_key"`
	FirstName string
	Age       int8
	LastName  *string
}

type TestCombinedModelIgnore struct {
	BaseEntity `eorm:"-"`
	Id         int64 `eorm:"primary_key"`
	FirstName  string
	Age        int8
	LastName   *string
}

type TestCombinedModelMulti struct {
	BaseEntity
	Id        int64 `eorm:"primary_key"`
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
	Id        int64 `eorm:"primary_key"`
	FirstName string
	Age       int8
	LastName  *string
}
