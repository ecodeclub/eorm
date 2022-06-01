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
		entity := &test.SimpleStruct{
			Bool: true, BoolPtr: test.ToPtr[bool](true),
			Int: -1, IntPtr: test.ToPtr[int](-1),
			Int8: -8, Int8Ptr: test.ToPtr[int8](-8),
			Int16: -16, Int16Ptr: test.ToPtr[int16](-16),
			Int32: -32, Int32Ptr: test.ToPtr[int32](-32),
			Int64: -64, Int64Ptr: test.ToPtr[int64](-64),
			Uint: 1, UintPtr: test.ToPtr[uint](1),
			Uint8: 8, Uint8Ptr: test.ToPtr[uint8](8),
			Uint16: 16, Uint16Ptr: test.ToPtr[uint16](16),
			Uint32: 32, Uint32Ptr: test.ToPtr[uint32](32),
			Uint64: 64, Uint64Ptr: test.ToPtr[uint64](64),
			Float32: 3.2, Float32Ptr: test.ToPtr[float32](3.2),
			Float64: 6.4, Float64Ptr: test.ToPtr[float64](6.4),
			Byte: 'a', BytePtr: test.ToPtr[byte]('a'), ByteArray: []byte{},
			String:         "hello",
			NullStringPtr:  &sql.NullString{String: "world", Valid: true},
			NullInt16Ptr:   &sql.NullInt16{Int16: -16, Valid: true},
			NullInt32Ptr:   &sql.NullInt32{Int32: -32, Valid: true},
			NullInt64Ptr:   &sql.NullInt64{Int64: -64, Valid: true},
			NullBoolPtr:    &sql.NullBool{Bool: true, Valid: true},
			NullBytePtr:    &sql.NullByte{Byte: 'b', Valid: true},
			NullTimePtr:    &sql.NullTime{Time: time.UnixMilli(1000), Valid: true},
			NullFloat64Ptr: &sql.NullFloat64{Float64: 6.4, Valid: true},
		}
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
			wantError: errs.NewInvalidColumnError("UpdateTime"),
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
			name: "byte",
			field: "Byte",
			wantVal: entity.Byte,
		},
		{
			name: "byte pointer",
			field: "BytePtr",
			wantVal: entity.BytePtr,
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
			name: "NullBoolPtr",
			field: "NullBoolPtr",
			wantVal: entity.NullBoolPtr,
		},
		{
			name: "NullBytePtr",
			field: "NullBytePtr",
			wantVal: entity.NullBytePtr,
		},
		{
			name: "NullTimePtr",
			field: "NullTimePtr",
			wantVal: entity.NullTimePtr,
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