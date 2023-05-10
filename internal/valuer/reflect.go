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
	"reflect"

	"github.com/ecodeclub/eorm/internal/rows"

	"github.com/ecodeclub/eorm/internal/errs"
	"github.com/ecodeclub/eorm/internal/model"
)

var _ Creator = NewReflectValue

// reflectValue 基于反射的 Value
type reflectValue struct {
	val  reflect.Value
	meta *model.TableMeta
}

// NewReflectValue 返回一个封装好的，基于反射实现的 Value
// 输入 val 必须是一个指向结构体实例的指针，而不能是任何其它类型
func NewReflectValue(val interface{}, meta *model.TableMeta) Value {
	return reflectValue{
		val:  reflect.ValueOf(val).Elem(),
		meta: meta,
	}
}

// Field 返回字段值
func (r reflectValue) Field(name string) (reflect.Value, error) {
	res, ok := r.fieldByIndex(name)
	if !ok {
		return reflect.Value{}, errs.NewInvalidFieldError(name)
	}
	return res, nil
}

func (r reflectValue) fieldByIndex(name string) (reflect.Value, bool) {
	cm, ok := r.meta.FieldMap[name]
	if !ok {
		return reflect.Value{}, false
	}
	value := r.val
	for _, i := range cm.FieldIndexes {
		value = value.Field(i)
	}
	return value, true
}

func (r reflectValue) SetColumns(rows rows.Rows) error {
	cs, err := rows.Columns()
	if err != nil {
		return err
	}
	if len(cs) > len(r.meta.Columns) {
		return errs.ErrTooManyColumns
	}

	// TODO 性能优化
	// colValues 和 colEleValues 实质上最终都指向同一个对象
	colValues := make([]interface{}, len(cs))
	colEleValues := make([]reflect.Value, len(cs))

	for i, c := range cs {
		cm, ok := r.meta.ColumnMap[c]
		if !ok {
			return errs.NewInvalidColumnError(c)
		}
		val := reflect.New(cm.Typ)
		colValues[i] = val.Interface()
		colEleValues[i] = val.Elem()
	}

	if err = rows.Scan(colValues...); err != nil {
		return err
	}

	for i, c := range cs {
		cm := r.meta.ColumnMap[c]
		fd, _ := r.fieldByIndex(cm.FieldName)
		fd.Set(colEleValues[i])
	}
	return nil
}
