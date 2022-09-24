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
	"unsafe"

	"github.com/gotomicro/eorm/internal/errs"
	"github.com/gotomicro/eorm/internal/model"
)

var _ Creator = NewUnsafeValue

type unsafeValue struct {
	addr unsafe.Pointer
	meta *model.TableMeta
}

func NewUnsafeValue(val interface{}, meta *model.TableMeta) Value {
	return unsafeValue{
		addr: unsafe.Pointer(reflect.ValueOf(val).Pointer()),
		meta: meta,
	}
}

func (u unsafeValue) Field(name string) (interface{}, error) {
	fd, ok := u.meta.FieldMap[name]
	if !ok {
		return nil, errs.NewInvalidFieldError(name)
	}
	ptr := unsafe.Pointer(uintptr(u.addr) + fd.Offset)
	val := reflect.NewAt(fd.Typ, ptr).Elem()
	return val.Interface(), nil
}

func (u unsafeValue) SetColumns(rows *sql.Rows) error {

	cs, err := rows.Columns()
	if err != nil {
		return err
	}
	if len(cs) > len(u.meta.Columns) {
		return errs.ErrTooManyColumns
	}

	// TODO 性能优化
	// colValues 和 colEleValues 实质上最终都指向同一个对象
	colValues := make([]interface{}, len(cs))
	for i, c := range cs {
		cm, ok := u.meta.ColumnMap[c]
		if !ok {
			return errs.NewInvalidColumnError(c)
		}
		ptr := unsafe.Pointer(uintptr(u.addr) + cm.Offset)
		val := reflect.NewAt(cm.Typ, ptr)
		colValues[i] = val.Interface()
	}
	return rows.Scan(colValues...)
}
