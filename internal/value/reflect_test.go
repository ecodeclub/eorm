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

package value

import (
	"errors"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestReflectValue_Field(t *testing.T) {
	ins := NewValue(&TestModel{
		Id: 13,
	})
	testCases := []struct{
		name string
		field string
		wantVal interface{}
		wantError error
	} {
		{
			name: "正常值",
			field: "Id",
			wantVal: int64(13),
		},
		{
			name: "不存在字段",
			field: "InvalidField",
			wantError: errors.New("eorm: 非法字段 InvalidField"),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			val, err := ins.Field(tc.field)
			assert.Equal(t, tc.wantError, err)
			if tc.wantError != nil {
				return
			}
			assert.Equal(t, tc.wantVal, val)
		})
	}
}

func FuzzReflectValue_Field(f *testing.F) {
	f.Fuzz(func(t *testing.T, wantId int64) {
		val := NewValue(&TestModel{
			Id: wantId,
		})
		id, err := val.Field("Id")
		assert.Nil(t, err)
		assert.Equal(t, wantId, id)
	})
}

func BenchmarkReflectValue_Field(b *testing.B) {
	ins := NewValue(&TestModel{
		Id: 13,
	})
	for i := 0; i < b.N; i++ {
		val, err := ins.Field("Id")
		assert.Nil(b, err)
		assert.Equal(b, int64(13), val)
	}
}

// TODO
// 添加更多的字段，覆盖以下类型
// uint 家族
// int 家族
// float 家族
// []byte
// string
type TestModel struct {
	Id int64
}