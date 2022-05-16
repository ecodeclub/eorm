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

	"github.com/stretchr/testify/assert"
)

func TestTagMetaRegistry(t *testing.T) {
	tm := &TestModel{}
	registry := &tagMetaRegistry{}
	meta, err := registry.Register(tm)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, 4, len(meta.Columns))
	assert.Equal(t, 4, len(meta.FieldMap))
	assert.Equal(t, reflect.TypeOf(tm), meta.Typ)
	assert.Equal(t, "test_model", meta.TableName)

	idMeta := meta.FieldMap["Id"]
	assert.Equal(t, "id", idMeta.ColumnName)
	assert.Equal(t, "Id", idMeta.FieldName)
	assert.Equal(t, reflect.TypeOf(int64(0)), idMeta.Typ)
	assert.True(t, idMeta.IsAutoIncrement)
	assert.True(t, idMeta.IsPrimaryKey)

	idMetaFistName := meta.FieldMap["FirstName"]
	assert.Equal(t, "first_name", idMetaFistName.ColumnName)
	assert.Equal(t, "FirstName", idMetaFistName.FieldName)
	assert.Equal(t, reflect.TypeOf(string("")), idMetaFistName.Typ)

	idMetaLastName := meta.FieldMap["LastName"]
	assert.Equal(t, "last_name", idMetaLastName.ColumnName)
	assert.Equal(t, "LastName", idMetaLastName.FieldName)
	assert.Equal(t, reflect.TypeOf((*string)(nil)), idMetaLastName.Typ)

	idMetaLastAge := meta.FieldMap["Age"]
	assert.Equal(t, "age", idMetaLastAge.ColumnName)
	assert.Equal(t, "Age", idMetaLastAge.FieldName)
	assert.Equal(t, reflect.TypeOf(int8(0)), idMetaLastAge.Typ)

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
	Id        int64 `eql:"auto_increment,primary_key,-"`
	FirstName string
	Age       int8 `eql:"-"`
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
	Id        int64 `eql:"auto_increment,primary_key"`
	FirstName string
	Age       int8
	LastName  *string
}