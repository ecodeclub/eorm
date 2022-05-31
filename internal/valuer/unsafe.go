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
	"reflect"
	"unsafe"
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
	if fd.IsHolderType {
		val := reflect.NewAt(fd.Typ, ptr).Elem()
		return val.Interface(), nil
	}
	switch fd.Typ.Kind() {
	case reflect.Bool:
		return *(*bool)(ptr), nil
	case reflect.Int:
		return *(*int)(ptr), nil
	case reflect.Int8:
		return *(*int8)(ptr), nil
	case reflect.Int16:
		return *(*int16)(ptr), nil
	case reflect.Int32:
		return *(*int32)(ptr), nil
	case reflect.Int64:
		return *(*int64)(ptr), nil
	case reflect.Uint:
		return *(*uint)(ptr), nil
	case reflect.Uint8:
		return *(*uint8)(ptr), nil
	case reflect.Uint16:
		return *(*uint16)(ptr), nil
	case reflect.Uint32:
		return *(*uint32)(ptr), nil
	case reflect.Uint64:
		return *(*uint64)(ptr), nil
	case reflect.Float32:
		return *(*float32)(ptr), nil
	case reflect.Float64:
		return *(*float64)(ptr), nil
	case reflect.String:
		return *(*string)(ptr), nil
	case reflect.Slice:
		// Array 只有一种可能，那就是 []byte
		return *(*[]byte)(ptr), nil
	case reflect.Pointer:
		ele := fd.Typ.Elem()
		switch ele.Kind() {
		case reflect.Bool:
			return *(**bool)(ptr), nil
		case reflect.Int:
			return *(**int)(ptr), nil
		case reflect.Int8:
			return *(**int8)(ptr), nil
		case reflect.Int16:
			return *(**int16)(ptr), nil
		case reflect.Int32:
			return *(**int32)(ptr), nil
		case reflect.Int64:
			return *(**int64)(ptr), nil
		case reflect.Uint:
			return *(**uint)(ptr), nil
		case reflect.Uint8:
			return *(**uint8)(ptr), nil
		case reflect.Uint16:
			return *(**uint16)(ptr), nil
		case reflect.Uint32:
			return *(**uint32)(ptr), nil
		case reflect.Uint64:
			return *(**uint64)(ptr), nil
		case reflect.Float32:
			return *(**float32)(ptr), nil
		case reflect.Float64:
			return *(**float64)(ptr), nil
		default:
			return nil, errs.NewUnsupportedTypeError(fd.Typ)
		}
	default:
		return nil, errs.NewUnsupportedTypeError(fd.Typ)
	}
}

func (u unsafeValue) SetColumn(column string, val *sql.RawBytes) error {
	cm, ok := u.meta.ColumnMap[column]
	if !ok {
		return errs.NewInvalidColumnError(column)
	}
	ptr := unsafe.Pointer(uintptr(u.addr) + cm.Offset)
	if cm.IsHolderType {
		scanner, err := decodeScanner(cm.Typ, val)
		if err != nil {
			return err
		}
		*(*uintptr)(ptr) = scanner.Pointer()
		return nil
	}
	cv, err := decode(cm.Typ, val)
	if err != nil {
		return err
	}
	if cv == nil {
		cv = reflect.Zero(cm.Typ).Interface()
	}
	switch cm.Typ.Kind() {
	case reflect.Bool:
		*(*bool)(ptr) = cv.(bool)
		return nil
	case reflect.Int:
		*(*int)(ptr) = cv.(int)
		return nil
	case reflect.Int8:
		*(*int8)(ptr) = cv.(int8)
		return nil
	case reflect.Int16:
		*(*int16)(ptr) = cv.(int16)
		return nil
	case reflect.Int32:
		*(*int32)(ptr) = cv.(int32)
		return nil
	case reflect.Int64:
		*(*int64)(ptr) = cv.(int64)
		return nil
	case reflect.Uint:
		*(*uint)(ptr) = cv.(uint)
		return nil
	case reflect.Uint8:
		*(*uint8)(ptr) = cv.(uint8)
		return nil
	case reflect.Uint16:
		*(*uint16)(ptr) = cv.(uint16)
		return nil
	case reflect.Uint32:
		*(*uint32)(ptr) = cv.(uint32)
		return nil
	case reflect.Uint64:
		*(*uint64)(ptr) = cv.(uint64)
		return nil
	case reflect.Float32:
		*(*float32)(ptr) = cv.(float32)
		return nil
	case reflect.Float64:
		*(*float64)(ptr) = cv.(float64)
		return nil
	case reflect.String:
		*(*string)(ptr) = cv.(string)
		return nil
	case reflect.Slice:
		// Array 只有一种可能，那就是 []byte
		*(*[]byte)(ptr) = cv.([]byte)
		return nil
	case reflect.Pointer:
		ele := cm.Typ.Elem()
		switch ele.Kind() {
		case reflect.Bool:
			*(**bool)(ptr) = cv.(*bool)
			return nil
		case reflect.Int:
			*(**int)(ptr) = cv.(*int)
			return nil
		case reflect.Int8:
			*(**int8)(ptr) = cv.(*int8)
			return nil
		case reflect.Int16:
			*(**int16)(ptr) = cv.(*int16)
			return nil
		case reflect.Int32:
			*(**int32)(ptr) = cv.(*int32)
			return nil
		case reflect.Int64:
			*(**int64)(ptr) = cv.(*int64)
			return nil
		case reflect.Uint:
			*(**uint)(ptr) = cv.(*uint)
			return nil
		case reflect.Uint8:
			*(**uint8)(ptr) = cv.(*uint8)
			return nil
		case reflect.Uint16:
			*(**uint16)(ptr) = cv.(*uint16)
			return nil
		case reflect.Uint32:
			*(**uint32)(ptr) = cv.(*uint32)
			return nil
		case reflect.Uint64:
			*(**uint64)(ptr) = cv.(*uint64)
			return nil
		case reflect.Float32:
			*(**float32)(ptr) = cv.(*float32)
			return nil
		case reflect.Float64:
			*(**float64)(ptr) = cv.(*float64)
			return nil
		default:
			return errs.NewUnsupportedTypeError(cm.Typ)
		}
	default:
		return errs.NewUnsupportedTypeError(cm.Typ)
	}
}
