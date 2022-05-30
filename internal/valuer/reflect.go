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
	"github.com/gotomicro/eorm/internal/errs"
	"github.com/gotomicro/eorm/internal/model"
	"reflect"
)

var _ Creator = NewReflectValue

// reflectValue 基于反射的 Value
type reflectValue struct {
	val reflect.Value
}

// NewReflectValue 返回一个封装好的，基于反射实现的 Value
// 输入 val 必须是一个指向结构体实例的指针，而不能是任何其它类型
func NewReflectValue(val interface{}, _ *model.TableMeta) Value {
	return reflectValue{
		val: reflect.ValueOf(val).Elem(),
	}
}

// Field 返回字段值
func (r reflectValue) Field(name string) (interface{}, error) {
	res := r.val.FieldByName(name)
	if res == (reflect.Value{}) {
		return nil, errs.NewInvalidColumnError(name)
	}
	return res.Interface(), nil
}
