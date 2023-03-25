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

package valuer

import (
	"database/sql"
	"database/sql/driver"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/ecodeclub/eorm/internal/errs"
	"github.com/ecodeclub/eorm/internal/model"
	"github.com/ecodeclub/eorm/internal/test"
	"github.com/stretchr/testify/assert"
)

func Test_primitiveValue_Field(t *testing.T) {
	testPrimitiveValueField(t, PrimitiveCreator{Creator: NewUnsafeValue})
	testPrimitiveValueField(t, PrimitiveCreator{Creator: NewReflectValue})
}

func testPrimitiveValueField(t *testing.T, creator PrimitiveCreator) {
	meta, err := model.NewMetaRegistry().Get(&test.SimpleStruct{})
	if err != nil {
		t.Fatal(err)
	}
	t.Run("zero value", func(t *testing.T) {
		entity := &test.SimpleStruct{}
		testCases := newValueFieldTestCases(entity)
		val := creator.NewPrimitiveValue(entity, meta)
		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				v, err := val.Field(tc.field)
				assert.Equal(t, tc.wantError, err)
				if err != nil {
					return
				}
				assert.Equal(t, tc.wantVal, v.Interface())
			})
		}
	})
	t.Run("normal value", func(t *testing.T) {
		entity := test.NewSimpleStruct(1)
		testCases := newValueFieldTestCases(entity)
		val := creator.NewPrimitiveValue(entity, meta)
		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				v, err := val.Field(tc.field)
				assert.Equal(t, tc.wantError, err)
				if err != nil {
					return
				}
				assert.Equal(t, tc.wantVal, v.Interface())
			})
		}
	})

	type User struct {
		CreateTime time.Time
		Name       *string
	}

	invalidCases := []valueFieldTestCase{
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

		val := creator.NewPrimitiveValue(&User{}, meta)
		for _, tc := range invalidCases {
			t.Run(tc.name, func(t *testing.T) {
				v, err := val.Field(tc.field)
				assert.Equal(t, tc.wantError, err)
				if err != nil {
					return
				}
				assert.Equal(t, tc.wantVal, v.Interface())
			})
		}
	})

	type BaseEntity struct {
		Id         int64 `eorm:"auto_increment,primary_key"`
		CreateTime uint64
	}
	type CombinedUser struct {
		BaseEntity
		FirstName string
	}

	cUser := &CombinedUser{}
	testCases := []valueFieldTestCase{
		{
			name:    "id",
			field:   "Id",
			wantVal: cUser.Id,
		},
		{
			name:    "CreateTime",
			field:   "CreateTime",
			wantVal: cUser.CreateTime,
		},
		{
			name:    "FirstName",
			field:   "FirstName",
			wantVal: cUser.FirstName,
		},
	}
	// 测试使用组合的场景
	t.Run("combination cases", func(t *testing.T) {
		meta, err = model.NewMetaRegistry().Get(cUser)
		if err != nil {
			t.Fatal(err)
		}

		val := creator.NewPrimitiveValue(cUser, meta)
		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				v, err := val.Field(tc.field)
				assert.Equal(t, tc.wantError, err)
				if err != nil {
					return
				}
				assert.Equal(t, tc.wantVal, v.Interface())
			})
		}
	})
}

func Test_primitiveValue_SetColumn(t *testing.T) {
	testCases := []struct {
		name       string
		cs         map[string][]byte
		val        any
		valCreator Creator
		wantVal    any
		wantErr    error
	}{
		{
			name:       "int",
			valCreator: NewUnsafeValue,
			cs: map[string][]byte{
				"id": []byte("10"),
			},
			val: new(int),
			wantVal: func() *int {
				val := 10
				return &val
			}(),
		},
		{
			name:       "string",
			valCreator: NewUnsafeValue,
			cs: map[string][]byte{
				"string": []byte("word"),
			},
			val: new(string),
			wantVal: func() *string {
				val := "word"
				return &val
			}(),
		},
		{
			name:       "bytes",
			valCreator: NewUnsafeValue,
			cs: map[string][]byte{
				"byte": []byte("word"),
			},
			val: new([]byte),
			wantVal: func() *[]byte {
				val := []byte("word")
				return &val
			}(),
		},
		{
			name:       "bool",
			valCreator: NewUnsafeValue,
			cs: map[string][]byte{
				"bool": []byte("true"),
			},
			val: new(bool),
			wantVal: func() *bool {
				val := true
				return &val
			}(),
		},
		{
			name:       "sql.NullString",
			valCreator: NewUnsafeValue,
			cs: map[string][]byte{
				"string": []byte("word"),
			},
			val:     &sql.NullString{},
			wantVal: &sql.NullString{String: "word", Valid: true},
		},
		{
			name:       "struct ptr value",
			valCreator: NewUnsafeValue,
			cs: map[string][]byte{
				"id":               []byte("1"),
				"bool":             []byte("true"),
				"bool_ptr":         []byte("false"),
				"int":              []byte("12"),
				"int_ptr":          []byte("13"),
				"int8":             []byte("8"),
				"int8_ptr":         []byte("-8"),
				"int16":            []byte("16"),
				"int16_ptr":        []byte("-16"),
				"int32":            []byte("32"),
				"int32_ptr":        []byte("-32"),
				"int64":            []byte("64"),
				"int64_ptr":        []byte("-64"),
				"uint":             []byte("14"),
				"uint_ptr":         []byte("15"),
				"uint8":            []byte("8"),
				"uint8_ptr":        []byte("18"),
				"uint16":           []byte("16"),
				"uint16_ptr":       []byte("116"),
				"uint32":           []byte("32"),
				"uint32_ptr":       []byte("132"),
				"uint64":           []byte("64"),
				"uint64_ptr":       []byte("164"),
				"float32":          []byte("3.2"),
				"float32_ptr":      []byte("-3.2"),
				"float64":          []byte("6.4"),
				"float64_ptr":      []byte("-6.4"),
				"byte_array":       []byte("hello"),
				"string":           []byte("world"),
				"null_string_ptr":  []byte("null string"),
				"null_int16_ptr":   []byte("16"),
				"null_int32_ptr":   []byte("32"),
				"null_int64_ptr":   []byte("64"),
				"null_bool_ptr":    []byte("true"),
				"null_float64_ptr": []byte("6.4"),
				"json_column":      []byte(`{"name": "Tom"}`),
			},
			val:     &test.SimpleStruct{},
			wantVal: test.NewSimpleStruct(1),
		},
		{
			name: "invalid field",
			cs: map[string][]byte{
				"invalid_column": nil,
			},
			valCreator: NewReflectValue,
			val:        &test.SimpleStruct{},
			wantErr:    errs.NewInvalidColumnError("invalid_column"),
		},
	}

	r := model.NewMetaRegistry()
	meta, err := r.Get(&test.SimpleStruct{})
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
			basicCreator := PrimitiveCreator{Creator: tc.valCreator}
			val := basicCreator.NewPrimitiveValue(tc.val, meta)
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
