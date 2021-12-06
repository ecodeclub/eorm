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
	assert.Equal(t, 4, len(meta.columns))
	assert.Equal(t, 4, len(meta.fieldMap))
	assert.Equal(t, reflect.TypeOf(tm), meta.typ)
	assert.Equal(t, "test_model", meta.tableName)

	idMeta := meta.fieldMap["Id"]
	assert.Equal(t, "id", idMeta.columnName)
	assert.Equal(t, "Id", idMeta.fieldName)
	assert.Equal(t, reflect.TypeOf(int64(0)), idMeta.typ)
	assert.True(t, idMeta.isAutoIncrement)
	assert.True(t, idMeta.isPrimaryKey)

	idMetaFistName := meta.fieldMap["FirstName"]
	assert.Equal(t, "first_name", idMetaFistName.columnName)
	assert.Equal(t, "FirstName", idMetaFistName.fieldName)
	assert.Equal(t, reflect.TypeOf(string("")), idMetaFistName.typ)

	idMetaLastName := meta.fieldMap["LastName"]
	assert.Equal(t, "last_name", idMetaLastName.columnName)
	assert.Equal(t, "LastName", idMetaLastName.fieldName)
	assert.Equal(t, reflect.TypeOf((*string)(nil)), idMetaLastName.typ)

	idMetaLastAge := meta.fieldMap["Age"]
	assert.Equal(t, "age", idMetaLastAge.columnName)
	assert.Equal(t, "Age", idMetaLastAge.fieldName)
	assert.Equal(t, reflect.TypeOf(int8(0)), idMetaLastAge.typ)

}

func TestIgnoreFieldsOption(t *testing.T) {
	tm := &TestIgnoreModel{}
	registry := &tagMetaRegistry{}
	meta, err := registry.Register(tm, IgnoreFieldsOption("Id", "FirstName"))
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, 1, len(meta.columns))
	assert.Equal(t, 1, len(meta.fieldMap))
	assert.Equal(t, reflect.TypeOf(tm), meta.typ)
	assert.Equal(t, "test_ignore_model", meta.tableName)

	_, hasId := meta.fieldMap["Id"]
	assert.False(t, hasId)

	_, hasFirstName := meta.fieldMap["FirstName"]
	assert.False(t, hasFirstName)

	_, hasAge := meta.fieldMap["Age"]
	assert.False(t, hasAge)

	_, hasLastName := meta.fieldMap["LastName"]
	assert.True(t, hasLastName)
}

type TestIgnoreModel struct {
	Id        int64 `eql:"auto_increment,primary_key,-"`
	FirstName string
	Age       int8 `eql:"-"`
	LastName  string
}
