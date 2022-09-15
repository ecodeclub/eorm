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
	"reflect"
	"testing"

	"github.com/gotomicro/eorm/internal/errs"
	"github.com/gotomicro/eorm/internal/model"
	"github.com/gotomicro/eorm/internal/test"
	"github.com/stretchr/testify/assert"
)

func Test_reflectValue_SetColumn(t *testing.T) {
	testSetColumn(t, NewReflectValue)
}

func TestReflectValue_Field(t *testing.T) {
	testCases := []struct {
		name    string
		input   string
		target  interface{}
		wantVal string
		wantErr error
	}{
		{
			// err
			name:    "err",
			input:   "firstName",
			target:  &TestModel{},
			wantErr: errs.NewInvalidColumnError("firstName"),
		},
		{
			// err2
			name:    "err2",
			input:   "firstname",
			target:  &TestModel{},
			wantErr: errs.NewInvalidColumnError("firstname"),
		},
		{
			// 普通
			name:    "Normal",
			input:   "FirstName",
			target:  &TestModel{},
			wantVal: "TestModel.FirstName",
		},
		{
			// 组合
			name:    "Combination",
			input:   "LastName",
			target:  &ConflictModel{},
			wantVal: "TestModel.LastName",
		},
		{
			// 组合2
			name:    "Combination",
			input:   "Age",
			target:  &ConflictModel{},
			wantVal: "Conflict.Age",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			registry := model.NewTagMetaRegistry()
			meta, err := registry.Register(tc.target)
			if err != nil {
				return
			}
			val := NewReflectValue(tc.target, meta)
			v, e := val.Field(tc.input)
			if e != nil {
				assert.Equal(t, tc.wantErr, e)
				return
			}
			assert.Equal(t, tc.wantVal, v)
		})
	}
}

// cpu: Intel(R) Core(TM) i7-6700HQ CPU @ 2.60GHz
// BenchmarkReflectValue_Field2
// BenchmarkReflectValue_Field2-8   	  131074	      8418 ns/op
func BenchmarkReflectValue_Field(b *testing.B) {
	// 普通
	registry := model.NewTagMetaRegistry()
	meta, err := registry.Register(&TestModel{})
	if err != nil {
		return
	}
	val := NewReflectValue(&TestModel{}, meta)
	for i := 0; i < b.N; i++ {
		v, _ := val.Field("FirstName")
		assert.Nil(b, err)
		assert.Equal(b, "TestModel.FirstName", v)
	}

	// 组合
	registry = model.NewTagMetaRegistry()
	meta, _ = registry.Register(&ProfileModel{})
	val = NewReflectValue(&ProfileModel{}, meta)
	for i := 0; i < b.N; i++ {
		v, _ := val.Field("LastName")
		assert.Equal(b, "TestModel.LastName", v)
	}

	registry = model.NewTagMetaRegistry()
	meta, _ = registry.Register(&ProfileModel{})
	val = NewReflectValue(&ProfileModel{}, meta)
	for i := 0; i < b.N; i++ {
		v, _ := val.Field("Email")
		assert.Equal(b, "ProfileModel.Email", v)
	}

	// 嵌套组合
	registry = model.NewTagMetaRegistry()
	meta, _ = registry.Register(&UserModel{})
	val = NewReflectValue(&UserModel{}, meta)
	for i := 0; i < b.N; i++ {
		v, _ := val.Field("Bio")
		assert.Equal(b, "UserModel.Bio", v)
	}

	registry = model.NewTagMetaRegistry()
	meta, _ = registry.Register(&UserModel{})
	val = NewReflectValue(&UserModel{}, meta)
	for i := 0; i < b.N; i++ {
		v, _ := val.Field("Id")
		assert.Equal(b, "TestModel.Id", v)
	}

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
		entity := &test.SimpleStruct{
			String: s,
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

type TestModel struct {
	Id        int64 `eorm:"auto_increment,primary_key"`
	FirstName string
	Age       int8
	LastName  string
}

type ProfileModel struct {
	Email    string
	Password string
	TestModel
}

type ConflictModel struct {
	TestModel
	Age int8
}

type UserModel struct {
	Bio string
	ProfileModel
}

func BenchmarkReflectValue_fieldByIndexes_VS_FieldByName(b *testing.B) {
	meta, _ := model.NewMetaRegistry().Get(&test.SimpleStruct{})
	ins := NewReflectValue(&test.SimpleStruct{Int64: 13}, meta)
	in, ok := ins.(reflectValue)
	assert.True(b, ok)
	fieldName, unknownFieldName := "Int64", "XXXX"
	fieldValue, unknownValue := int64(13), reflect.Value{}
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
