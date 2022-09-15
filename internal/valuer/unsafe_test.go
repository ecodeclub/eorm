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

package valuer

import (
	"database/sql/driver"
	"reflect"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/gotomicro/eorm/internal/errs"
	"github.com/gotomicro/eorm/internal/model"
	"github.com/gotomicro/eorm/internal/test"
	"github.com/stretchr/testify/assert"
)

func Test_unsafeValue_Field(t *testing.T) {
	testValueField(t, NewUnsafeValue)
}

func testValueField(t *testing.T, creator Creator) {
	meta, err := model.NewMetaRegistry().Get(&test.SimpleStruct{})
	if err != nil {
		t.Fatal(err)
	}
	t.Run("zero value", func(t *testing.T) {
		entity := &test.SimpleStruct{}
		testCases := newValueFieldTestCases(entity)
		val := creator(entity, meta)
		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				v, err := val.Field(tc.field)
				assert.Equal(t, tc.wantError, err)
				if err != nil {
					return
				}
				assert.Equal(t, tc.wantVal, v)
			})
		}
	})
	t.Run("normal value", func(t *testing.T) {
		entity := test.NewSimpleStruct(1)
		testCases := newValueFieldTestCases(entity)
		val := NewUnsafeValue(entity, meta)
		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				v, err := val.Field(tc.field)
				assert.Equal(t, tc.wantError, err)
				if err != nil {
					return
				}
				assert.Equal(t, tc.wantVal, v)
			})
		}
	})

	type User struct {
		CreateTime time.Time
		Name       string
	}

	invalidCases := []valueFieldTestCase{
		{
			// 不支持的字段类型
			name:      "invalid type",
			field:     "CreateTime",
			wantError: errs.NewUnsupportedTypeError(reflect.TypeOf(time.Now())),
		},
		{
			// 不存在的字段
			name:      "invalid field",
			field:     "UpdateTime",
			wantError: errs.NewInvalidFieldError("UpdateTime"),
		},
	}
	t.Run("invalid cases", func(t *testing.T) {
		meta, err := model.NewMetaRegistry().Get(&User{})
		if err != nil {
			t.Fatal(err)
		}

		val := NewUnsafeValue(&User{}, meta)
		for _, tc := range invalidCases {
			t.Run(tc.name, func(t *testing.T) {
				v, err := val.Field(tc.field)
				assert.Equal(t, tc.wantError, err)
				if err != nil {
					return
				}
				assert.Equal(t, tc.wantVal, v)
			})
		}
	})
}

func Test_unsafeValue_SetColumn(t *testing.T) {
	testSetColumn(t, NewUnsafeValue)
}

func testSetColumn(t *testing.T, creator Creator) {
	testCases := []struct {
		name    string
		cs      map[string][]byte
		val     *test.SimpleStruct
		wantVal *test.SimpleStruct
		wantErr error
	}{
		{
			name: "normal value",
			cs: map[string][]byte{
				"id":         []byte("1"),
				"bool":       []byte("true"),
				"int":        []byte("12"),
				"int8":       []byte("8"),
				"int16":      []byte("16"),
				"int32":      []byte("32"),
				"int64":      []byte("64"),
				"uint":       []byte("14"),
				"uint8":      []byte("8"),
				"uint16":     []byte("16"),
				"uint32":     []byte("32"),
				"uint64":     []byte("64"),
				"float32":    []byte("3.2"),
				"float64":    []byte("6.4"),
				"byte_array": []byte("hello"),
				"string":     []byte("world"),
			},
			val:     &test.SimpleStruct{},
			wantVal: test.NewSimpleStruct(1),
		},
		{
			name: "invalid field",
			cs: map[string][]byte{
				"invalid_column": nil,
			},
			wantErr: errs.NewInvalidColumnError("invalid_column"),
		},
	}

	meta, err := model.NewMetaRegistry().Get(&test.SimpleStruct{})
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = db.Close() }()
			val := creator(tc.val, meta)
			cols := make([]string, 0, len(tc.cs))
			colVals := make([]driver.Value, 0, len(tc.cs))
			for k, v := range tc.cs {
				cols = append(cols, k)
				colVals = append(colVals, v)
			}
			mock.ExpectQuery("SELECT *").
				WillReturnRows(sqlmock.NewRows(cols).
					AddRow(colVals...))
			rows, _ := db.Query("SELECT *")
			rows.Next()
			err = val.SetColumns(rows)
			if err != nil {
				assert.Equal(t, tc.wantErr, err)
				return
			}
			if tc.wantErr != nil {
				t.Fatalf("期望得到错误，但是并没有得到 %v", tc.wantErr)
			}
			assert.Equal(t, tc.wantVal, tc.val)
		})
	}

}

func newValueFieldTestCases(entity *test.SimpleStruct) []valueFieldTestCase {
	return []valueFieldTestCase{
		{
			name:    "bool",
			field:   "Bool",
			wantVal: entity.Bool,
		},
		{
			name:    "int",
			field:   "Int",
			wantVal: entity.Int,
		},
		{
			name:    "int8",
			field:   "Int8",
			wantVal: entity.Int8,
		},
		{
			name:    "int16",
			field:   "Int16",
			wantVal: entity.Int16,
		},
		{
			name:    "int32",
			field:   "Int32",
			wantVal: entity.Int32,
		},
		{
			name:    "int64",
			field:   "Int64",
			wantVal: entity.Int64,
		},
		{
			name:    "uint",
			field:   "Uint",
			wantVal: entity.Uint,
		},
		{
			name:    "uint8",
			field:   "Uint8",
			wantVal: entity.Uint8,
		},
		{
			name:    "uint16",
			field:   "Uint16",
			wantVal: entity.Uint16,
		},
		{
			name:    "uint32",
			field:   "Uint32",
			wantVal: entity.Uint32,
		},
		{
			name:    "uint64",
			field:   "Uint64",
			wantVal: entity.Uint64,
		},
		{
			name:    "float32",
			field:   "Float32",
			wantVal: entity.Float32,
		},
		{
			name:    "float64",
			field:   "Float64",
			wantVal: entity.Float64,
		},
		{
			name:    "byte array",
			field:   "ByteArray",
			wantVal: entity.ByteArray,
		},
		{
			name:    "string",
			field:   "String",
			wantVal: entity.String,
		},
	}
}

type valueFieldTestCase struct {
	name      string
	field     string
	wantVal   interface{}
	wantError error
}
