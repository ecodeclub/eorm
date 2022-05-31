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
	"database/sql"
	"github.com/gotomicro/ekit"
	"github.com/gotomicro/eorm/internal/errs"
	"github.com/gotomicro/eorm/internal/model"
	"github.com/gotomicro/eorm/internal/test"
	"github.com/stretchr/testify/assert"
	"reflect"
	"testing"
	"time"
)

func Test_unsafeValue_Field(t *testing.T) {
	testValueField(t, NewUnsafeValue)
}

func Test_unsafeValue_SetColumn(t *testing.T) {
	testSetColumn(t, NewUnsafeValue)
}

func testSetColumn(t *testing.T, creator Creator) {
	testCases := []struct{
		name string
		cs map[string]*sql.RawBytes
		val *test.SimpleStruct
		wantVal *test.SimpleStruct
		wantErr error
	} {
		{
			// 零值会覆盖掉已有的值
			name: "nil override",
			cs: map[string]*sql.RawBytes{
				"id": nil,
				"bool": nil,
				"bool_ptr": nil,
				"int": nil,
				"int_ptr": nil,
				"int8": nil,
				"int8_ptr": nil,
				"int16": nil,
				"int16_ptr": nil,
				"int32": nil,
				"int32_ptr": nil,
				"int64": nil,
				"int64_ptr": nil,
				"uint": nil,
				"uint_ptr": nil,
				"uint8": nil,
				"uint8_ptr": nil,
				"uint16": nil,
				"uint16_ptr": nil,
				"uint32": nil,
				"uint32_ptr": nil,
				"uint64": nil,
				"uint64_ptr": nil,
				"float32": nil,
				"float32_ptr": nil,
				"float64": nil,
				"float64_ptr": nil,
				"byte_array": nil,
				"string": nil,
				"null_string_ptr": nil,
				"null_int16_ptr": nil,
				"null_int32_ptr": nil,
				"null_int64_ptr": nil,
				"null_bool_ptr": nil,
				"null_float64_ptr": nil,
				"json_column": nil,
			},
			val: test.NewSimpleStruct(1),
			wantVal: &test.SimpleStruct{},
		},
		{
			// 零值会覆盖掉已有的值，这里的零值是带类型的零值
			// 这个测试有点冗余，但是依旧保留
			name: "bs nil override",
			cs: map[string]*sql.RawBytes{
				"id": (*sql.RawBytes)(nil),
				"bool": (*sql.RawBytes)(nil),
				"bool_ptr": (*sql.RawBytes)(nil),
				"int": (*sql.RawBytes)(nil),
				"int_ptr": (*sql.RawBytes)(nil),
				"int8": (*sql.RawBytes)(nil),
				"int8_ptr": (*sql.RawBytes)(nil),
				"int16": (*sql.RawBytes)(nil),
				"int16_ptr": (*sql.RawBytes)(nil),
				"int32": (*sql.RawBytes)(nil),
				"int32_ptr": (*sql.RawBytes)(nil),
				"int64": (*sql.RawBytes)(nil),
				"int64_ptr": (*sql.RawBytes)(nil),
				"uint": (*sql.RawBytes)(nil),
				"uint_ptr": (*sql.RawBytes)(nil),
				"uint8": (*sql.RawBytes)(nil),
				"uint8_ptr": (*sql.RawBytes)(nil),
				"uint16": (*sql.RawBytes)(nil),
				"uint16_ptr": (*sql.RawBytes)(nil),
				"uint32": (*sql.RawBytes)(nil),
				"uint32_ptr": (*sql.RawBytes)(nil),
				"uint64": (*sql.RawBytes)(nil),
				"uint64_ptr": (*sql.RawBytes)(nil),
				"float32": (*sql.RawBytes)(nil),
				"float32_ptr": (*sql.RawBytes)(nil),
				"float64": (*sql.RawBytes)(nil),
				"float64_ptr": (*sql.RawBytes)(nil),
				"byte_array": (*sql.RawBytes)(nil),
				"string": (*sql.RawBytes)(nil),
				"null_string_ptr": (*sql.RawBytes)(nil),
				"null_int16_ptr": (*sql.RawBytes)(nil),
				"null_int32_ptr": (*sql.RawBytes)(nil),
				"null_int64_ptr": (*sql.RawBytes)(nil),
				"null_bool_ptr": (*sql.RawBytes)(nil),
				"null_float64_ptr": (*sql.RawBytes)(nil),
				"json_column": (*sql.RawBytes)(nil),
			},
			val: test.NewSimpleStruct(1),
			wantVal: &test.SimpleStruct{},
		},
		{
			name: "normal value",
			cs: map[string]*sql.RawBytes{
				"id": ekit.ToPtr[sql.RawBytes]([]byte("1")),
				"bool": ekit.ToPtr[sql.RawBytes]([]byte("true")),
				"bool_ptr": ekit.ToPtr[sql.RawBytes]([]byte("false")),
				"int": ekit.ToPtr[sql.RawBytes]([]byte("12")),
				"int_ptr": ekit.ToPtr[sql.RawBytes]([]byte("13")),
				"int8": ekit.ToPtr[sql.RawBytes]([]byte("8")),
				"int8_ptr": ekit.ToPtr[sql.RawBytes]([]byte("-8")),
				"int16": ekit.ToPtr[sql.RawBytes]([]byte("16")),
				"int16_ptr": ekit.ToPtr[sql.RawBytes]([]byte("-16")),
				"int32": ekit.ToPtr[sql.RawBytes]([]byte("32")),
				"int32_ptr": ekit.ToPtr[sql.RawBytes]([]byte("-32")),
				"int64": ekit.ToPtr[sql.RawBytes]([]byte("64")),
				"int64_ptr": ekit.ToPtr[sql.RawBytes]([]byte("-64")),
				"uint": ekit.ToPtr[sql.RawBytes]([]byte("14")),
				"uint_ptr": ekit.ToPtr[sql.RawBytes]([]byte("15")),
				"uint8": ekit.ToPtr[sql.RawBytes]([]byte("8")),
				"uint8_ptr": ekit.ToPtr[sql.RawBytes]([]byte("18")),
				"uint16": ekit.ToPtr[sql.RawBytes]([]byte("16")),
				"uint16_ptr": ekit.ToPtr[sql.RawBytes]([]byte("116")),
				"uint32": ekit.ToPtr[sql.RawBytes]([]byte("32")),
				"uint32_ptr": ekit.ToPtr[sql.RawBytes]([]byte("132")),
				"uint64": ekit.ToPtr[sql.RawBytes]([]byte("64")),
				"uint64_ptr": ekit.ToPtr[sql.RawBytes]([]byte("164")),
				"float32": ekit.ToPtr[sql.RawBytes]([]byte("3.2")),
				"float32_ptr": ekit.ToPtr[sql.RawBytes]([]byte("-3.2")),
				"float64": ekit.ToPtr[sql.RawBytes]([]byte("6.4")),
				"float64_ptr": ekit.ToPtr[sql.RawBytes]([]byte("-6.4")),
				"byte_array": ekit.ToPtr[sql.RawBytes]([]byte("hello")),
				"string": ekit.ToPtr[sql.RawBytes]([]byte("world")),
				"null_string_ptr": ekit.ToPtr[sql.RawBytes]([]byte("null string")),
				"null_int16_ptr": ekit.ToPtr[sql.RawBytes]([]byte("16")),
				"null_int32_ptr": ekit.ToPtr[sql.RawBytes]([]byte("32")),
				"null_int64_ptr": ekit.ToPtr[sql.RawBytes]([]byte("64")),
				"null_bool_ptr": ekit.ToPtr[sql.RawBytes]([]byte("true")),
				"null_float64_ptr": ekit.ToPtr[sql.RawBytes]([]byte("6.4")),
				"json_column": ekit.ToPtr[sql.RawBytes]([]byte(`{"name": "Tom"}`)),
			},
			val: &test.SimpleStruct{},
			wantVal: test.NewSimpleStruct(1),
		},
		{
			name:"invalid field",
			cs: map[string]*sql.RawBytes{
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
			val := creator(tc.val, meta)
			for c, bs := range tc.cs {
				err = val.SetColumn(c, bs)
				if err != nil {
					assert.Equal(t, tc.wantErr, err)
					return
				}
			}
			if tc.wantErr != nil {
				t.Fatalf("期望得到错误，但是并没有得到 %v", tc.wantErr)
			}
			assert.Equal(t, tc.wantVal, tc.val)
		})
	}

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
		Name *string
	}

	invalidCases := []valueFieldTestCase{
		{
			// 不支持的字段类型
			name: "invalid type",
			field: "CreateTime",
			wantError: errs.NewUnsupportedTypeError(reflect.TypeOf(time.Now())),
		},
		{
			// 不支持的指针类型
			name: "invalid pointer type",
			field: "Name",
			wantError: errs.NewUnsupportedTypeError(reflect.TypeOf((*string)(nil))),
		},
		{
			// 不存在的字段
			name: "invalid field",
			field: "UpdateTime",
			wantError: errs.NewInvalidFieldError("UpdateTime"),
		},
	}
	t.Run("invalid cases", func(t *testing.T) {
		meta, err := model.NewMetaRegistry().Get(&User{})
		if err !=nil {
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

func newValueFieldTestCases(entity *test.SimpleStruct) []valueFieldTestCase{
	return []valueFieldTestCase{
		{
			name: "bool",
			field: "Bool",
			wantVal: entity.Bool,
		},
		{
			// bool 指针类型
			name: "bool pointer",
			field: "BoolPtr",
			wantVal: entity.BoolPtr,
		},
		{
			name: "int",
			field: "Int",
			wantVal: entity.Int,
		},
		{
			// int 指针类型
			name: "int pointer",
			field: "IntPtr",
			wantVal: entity.IntPtr,
		},
		{
			name: "int8",
			field: "Int8",
			wantVal: entity.Int8,
		},
		{
			name: "int8 pointer",
			field: "Int8Ptr",
			wantVal: entity.Int8Ptr,
		},
		{
			name: "int16",
			field: "Int16",
			wantVal: entity.Int16,
		},
		{
			name: "int16 pointer",
			field: "Int16Ptr",
			wantVal: entity.Int16Ptr,
		},
		{
			name: "int32",
			field: "Int32",
			wantVal: entity.Int32,
		},
		{
			name: "int32 pointer",
			field: "Int32Ptr",
			wantVal: entity.Int32Ptr,
		},
		{
			name: "int64",
			field: "Int64",
			wantVal: entity.Int64,
		},
		{
			name: "int64 pointer",
			field: "Int64Ptr",
			wantVal: entity.Int64Ptr,
		},
		{
			name: "uint",
			field: "Uint",
			wantVal: entity.Uint,
		},
		{
			name: "uint pointer",
			field: "UintPtr",
			wantVal: entity.UintPtr,
		},
		{
			name: "uint8",
			field: "Uint8",
			wantVal: entity.Uint8,
		},
		{
			name: "uint8 pointer",
			field: "Uint8Ptr",
			wantVal: entity.Uint8Ptr,
		},
		{
			name: "uint16",
			field: "Uint16",
			wantVal: entity.Uint16,
		},
		{
			name: "uint16 pointer",
			field: "Uint16Ptr",
			wantVal: entity.Uint16Ptr,
		},
		{
			name: "uint32",
			field: "Uint32",
			wantVal: entity.Uint32,
		},
		{
			name: "uint32 pointer",
			field: "Uint32Ptr",
			wantVal: entity.Uint32Ptr,
		},
		{
			name: "uint64",
			field: "Uint64",
			wantVal: entity.Uint64,
		},
		{
			name: "uint64 pointer",
			field: "Uint64Ptr",
			wantVal: entity.Uint64Ptr,
		},
		{
			name: "float32",
			field: "Float32",
			wantVal: entity.Float32,
		},
		{
			name: "float32 pointer",
			field: "Float32Ptr",
			wantVal: entity.Float32Ptr,
		},
		{
			name: "float64",
			field: "Float64",
			wantVal: entity.Float64,
		},
		{
			name: "float64 pointer",
			field: "Float64Ptr",
			wantVal: entity.Float64Ptr,
		},
		{
			name: "byte array",
			field: "ByteArray",
			wantVal: entity.ByteArray,
		},
		{
			name: "string",
			field: "String",
			wantVal: entity.String,
		},
		{
			name: "NullStringPtr",
			field: "NullStringPtr",
			wantVal: entity.NullStringPtr,
		},
		{
			name: "NullInt16Ptr",
			field: "NullInt16Ptr",
			wantVal: entity.NullInt16Ptr,
		},
		{
			name: "NullInt32Ptr",
			field: "NullInt32Ptr",
			wantVal: entity.NullInt32Ptr,
		},
		{
			name: "NullInt64Ptr",
			field: "NullInt64Ptr",
			wantVal: entity.NullInt64Ptr,
		},
		{
			name: "NullFloat64Ptr",
			field: "NullFloat64Ptr",
			wantVal: entity.NullFloat64Ptr,
		},
		{
			name: "JsonColumn",
			field: "JsonColumn",
			wantVal: entity.JsonColumn,
		},
	}
}

type valueFieldTestCase struct {
	name string
	field string
	wantVal interface{}
	wantError error
}

func FuzzUnsafeValue_Field(f *testing.F) {
	f.Fuzz(fuzzValueField(NewUnsafeValue))
}

func BenchmarkUnsafeValue_Field(b *testing.B) {
	meta, _ := model.NewMetaRegistry().Get(&test.SimpleStruct{})
	ins := NewReflectValue(&test.SimpleStruct{ Int64: 13, NullStringPtr: &sql.NullString{}}, meta)
	b.Run("int64", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			val, err := ins.Field("Int64")
			assert.Nil(b, err)
			assert.Equal(b, int64(13), val)
		}
	})
	b.Run("holder", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			val, err := ins.Field("NullStringPtr")
			assert.Nil(b, err)
			assert.Equal(b, &sql.NullString{}, val)
		}
	})
}