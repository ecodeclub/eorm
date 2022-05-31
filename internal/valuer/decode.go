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
	"reflect"
	"strconv"
	"unsafe"
)

var (
	intSize = int(unsafe.Sizeof(0))
	uintSize = int(unsafe.Sizeof(uint(0)))
)


// TODO 以下这些方法在将来的时候考虑转化为接口，从而允许使用的不同的实现

func decode(typ reflect.Type, bs *sql.RawBytes) (interface{}, error) {
	if bs == nil {
		return nil, nil
	}
	switch typ.Kind() {
	case reflect.Bool:
		return decodeBool(bs)
	case reflect.Int:
		return decodeInt(bs)
	case reflect.Int8:
		return decodeInt8(bs)
	case reflect.Int16:
		return decodeInt16(bs)
	case reflect.Int32:
		return decodeInt32(bs)
	case reflect.Int64:
		return decodeInt64(bs)
	case reflect.Uint:
		return decodeUint(bs)
	case reflect.Uint8:
		return decodeUint8(bs)
	case reflect.Uint16:
		return decodeUint16(bs)
	case reflect.Uint32:
		return decodeUint32(bs)
	case reflect.Uint64:
		return decodeUint64(bs)
	case reflect.Float32:
		return decodeFloat32(bs)
	case reflect.Float64:
		return decodeFloat64(bs)
	case reflect.String:
		return decodeString(bs)
	case reflect.Slice:
		// Array 只有一种可能，那就是 []byte
		return decodeSlice(bs)
	case reflect.Pointer:
		ele := typ.Elem()
		switch ele.Kind() {
		case reflect.Bool:
			res, err := decodeBool(bs)
			return &res, err
		case reflect.Int:
			res, err := decodeInt(bs)
			return &res, err
		case reflect.Int8:
			res, err := decodeInt8(bs)
			return &res, err
		case reflect.Int16:
			res, err := decodeInt16(bs)
			return &res, err
		case reflect.Int32:
			res, err := decodeInt32(bs)
			return &res, err
		case reflect.Int64:
			res, err := decodeInt64(bs)
			return &res, err
		case reflect.Uint:
			res, err := decodeUint(bs)
			return &res, err
		case reflect.Uint8:
			res, err := decodeUint8(bs)
			return &res, err
		case reflect.Uint16:
			res, err := decodeUint16(bs)
			return &res, err
		case reflect.Uint32:
			res, err := decodeUint32(bs)
			return &res, err
		case reflect.Uint64:
			res, err := decodeUint64(bs)
			return &res, err
		case reflect.Float32:
			res, err := decodeFloat32(bs)
			return &res, err
		case reflect.Float64:
			res, err := decodeFloat64(bs)
			return &res, err
		default:
			return nil, errs.NewUnsupportedTypeError(typ)
		}
	default:
		return nil, errs.NewUnsupportedTypeError(typ)
	}
}

func decodeBool(bs *sql.RawBytes) (bool, error) {
	return strconv.ParseBool(unsafeConvertToString(bs))
}

func decodeInt(bs *sql.RawBytes) (int, error) {
	res, err := strconv.ParseInt(unsafeConvertToString(bs), 10, intSize)
	return int(res), err
}

func decodeInt8(bs *sql.RawBytes) (int8, error) {
	res, err := strconv.ParseInt(unsafeConvertToString(bs), 10, 8)
	return int8(res), err
}

func decodeInt16(bs *sql.RawBytes) (int16, error) {
	res, err := strconv.ParseInt(unsafeConvertToString(bs), 10, 16)
	return int16(res), err
}

func decodeInt32(bs *sql.RawBytes) (int32, error) {
	res, err := strconv.ParseInt(unsafeConvertToString(bs), 10, 32)
	return int32(res), err
}

func decodeInt64(bs *sql.RawBytes) (int64, error) {
	return strconv.ParseInt(unsafeConvertToString(bs), 10, 64)
}

func decodeUint(bs *sql.RawBytes) (uint, error) {
	res, err := strconv.ParseUint(unsafeConvertToString(bs), 10, uintSize)
	return uint(res), err
}

func decodeUint8(bs *sql.RawBytes) (uint8, error) {
	res, err := strconv.ParseUint(unsafeConvertToString(bs), 10, 8)
	return uint8(res), err
}

func decodeUint16(bs *sql.RawBytes) (uint16, error) {
	res, err := strconv.ParseUint(unsafeConvertToString(bs), 10, 16)
	return uint16(res), err
}

func decodeUint32(bs *sql.RawBytes) (uint32, error) {
	res, err := strconv.ParseUint(unsafeConvertToString(bs), 10, 32)
	return uint32(res), err
}

func decodeUint64(bs *sql.RawBytes) (uint64, error) {
	return  strconv.ParseUint(unsafeConvertToString(bs), 10, 64)
}

func decodeFloat32(bs *sql.RawBytes) (float32, error) {
	res, err := strconv.ParseFloat(unsafeConvertToString(bs), 32)
	return float32(res), err
}

func decodeFloat64(bs *sql.RawBytes) (float64, error) {
	return strconv.ParseFloat(unsafeConvertToString(bs), 64)
}

func decodeString(bs *sql.RawBytes) (string, error) {
	return string(*bs), nil
}

func decodeSlice(bs *sql.RawBytes) ([]byte, error) {
	res := make([]byte, len(*bs))
	copy(res, *bs)
	return res, nil
}

func decodeScanner(typ reflect.Type, val *sql.RawBytes) (reflect.Value, error) {
	if val == nil {
		return reflect.Zero(typ), nil
	}
	scanner := reflect.New(typ.Elem())
	valCoy := make([]byte, len(*val))
	copy(valCoy, *val)
	if err := scanner.Interface().(sql.Scanner).Scan(valCoy); err != nil {
		return reflect.Value{}, err
	}
	return scanner, nil
}

func unsafeConvertToString(bs *sql.RawBytes) string {
	return *(*string)(unsafe.Pointer(bs))
}


