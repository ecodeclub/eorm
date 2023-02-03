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

	"github.com/gotomicro/eorm/internal/model"
)

// supportBasicTypeValue 支持基本类型 Value
type supportBasicTypeValue struct {
	Value
	val     any
	valType reflect.Type
}

// Field 返回字段值
func (s supportBasicTypeValue) Field(name string) (reflect.Value, error) {
	return s.Value.Field(name)
}

// SetColumns 设置列值， 支持基本类型，基于 reflect 与 unsafe Value 封装
func (s supportBasicTypeValue) SetColumns(rows *sql.Rows) error {
	switch s.valType.Elem().Kind() {
	case reflect.Struct:
		if scanner, ok := s.val.(sql.Scanner); ok {
			return rows.Scan(scanner)
		}
		return s.Value.SetColumns(rows)
	default:
		return rows.Scan(s.val)
	}
}

// BasicTypeCreator 支持基本类型的 Creator, 基于原生的 Creator 扩展
type BasicTypeCreator struct {
	Creator
}

// NewBasicTypeValue 返回一个封装好的，基于支持基本类型实现的 Value
// 输入 val 必须是一个指向结构体实例的指针，而不能是任何其它类型
func (c BasicTypeCreator) NewBasicTypeValue(val any, meta *model.TableMeta) Value {
	return supportBasicTypeValue{
		val:     val,
		Value:   c.Creator(val, meta),
		valType: reflect.TypeOf(val),
	}
}
