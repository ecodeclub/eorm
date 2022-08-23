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
	"reflect"
	"testing"

	"github.com/gotomicro/eorm/internal/errs"
	"github.com/gotomicro/eorm/internal/model"
	"github.com/gotomicro/eorm/internal/test"
	"github.com/stretchr/testify/assert"
)

func TestReflectValue_Field(t *testing.T) {
	testValueField(t, NewReflectValue)
	invalidCases := []valueFieldTestCase{
		{
			// 不存在的字段
			name:      "invalid field",
			field:     "UpdateTime",
			wantError: errs.NewInvalidFieldError("UpdateTime"),
		},
	}
	t.Run("invalid cases", func(t *testing.T) {
		meta, err := model.NewMetaRegistry().Get(&test.SimpleStruct{})
		if err != nil {
			t.Fatal(err)
		}
		val := NewReflectValue(&test.SimpleStruct{}, meta)
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

func Test_reflectValue_SetColumn(t *testing.T) {
	testSetColumn(t, NewReflectValue)
}

func FuzzReflectValue_Field(f *testing.F) {
	f.Fuzz(fuzzValueField(NewReflectValue))
}

func fuzzValueField(factory Creator) any {
	meta, _ := model.NewMetaRegistry().Get(&test.SimpleStruct{})
	return func(t *testing.T, b bool,
		i int, i8 int8, i16 int16, i32 int32, i64 int64,
		u uint, u8 uint8, u16 uint16, u32 uint32, u64 uint64,
		f32 float32, f64 float64, bt byte, bs []byte, s string) {
		cb := b
		entity := &test.SimpleStruct{
			Bool: b, BoolPtr: &cb,
			Int: i, IntPtr: &i,
			Int8: i8, Int8Ptr: &i8,
			Int16: i16, Int16Ptr: &i16,
			Int32: i32, Int32Ptr: &i32,
			Int64: i64, Int64Ptr: &i64,
			Uint: u, UintPtr: &u,
			Uint8: u8, Uint8Ptr: &u8,
			Uint16: u16, Uint16Ptr: &u16,
			Uint32: u32, Uint32Ptr: &u32,
			Uint64: u64, Uint64Ptr: &u64,
			Float32: f32, Float32Ptr: &f32,
			Float64: f64, Float64Ptr: &f64,
			String:         s,
			NullStringPtr:  &sql.NullString{String: s, Valid: b},
			NullInt16Ptr:   &sql.NullInt16{Int16: i16, Valid: b},
			NullInt32Ptr:   &sql.NullInt32{Int32: i32, Valid: b},
			NullInt64Ptr:   &sql.NullInt64{Int64: i64, Valid: b},
			NullBoolPtr:    &sql.NullBool{Bool: b, Valid: b},
			NullFloat64Ptr: &sql.NullFloat64{Float64: f64, Valid: b},
		}
		val := factory(entity, meta)
		cases := newValueFieldTestCases(entity)
		for _, c := range cases {
			v, err := val.Field(c.field)
			assert.Nil(t, err)
			assert.Equal(t, c.wantVal, v)
		}
	}
}

func BenchmarkReflectValue_Field(b *testing.B) {
	meta, _ := model.NewMetaRegistry().Get(&test.SimpleStruct{})
	ins := NewReflectValue(&test.SimpleStruct{Int64: 13}, meta)
	for i := 0; i < b.N; i++ {
		val, err := ins.Field("Int64")
		assert.Nil(b, err)
		assert.Equal(b, int64(13), val)
	}
}

func BenchmarkReflectValue_fieldByIndexes_VS_FieldByName(b *testing.B) {
	meta, _ := model.NewMetaRegistry().Get(&test.SimpleStruct{})
	ins := NewReflectValue(&test.SimpleStruct{Int64: 13}, meta)
	in, ok := ins.(reflectValue)
	assert.True(b, ok)
	fieldName, unknownFieldName := "Int64", "XXXX"
	fieldValue, unknownValue := int64(13), reflect.Value{}
	b.Run("fieldByIndex found", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			val, ok := in.fieldByIndex(fieldName)
			assert.True(b, ok)
			assert.Equal(b, fieldValue, val.Interface())
		}
	})
	b.Run("fieldByIndex not found", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			val, ok := in.fieldByIndex(unknownFieldName)
			assert.False(b, ok)
			assert.Equal(b, unknownValue, val)
		}
	})
	b.Run("FieldByName found", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			val := in.val.FieldByName(fieldName)
			assert.Equal(b, fieldValue, val.Interface())
		}
	})
	b.Run("fieldByIndex not found", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			val := in.val.FieldByName(unknownFieldName)
			assert.Equal(b, unknownValue, val)
		}
	})
}
