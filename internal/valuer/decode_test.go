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
	"github.com/gotomicro/ekit"
	"github.com/gotomicro/eorm/internal/errs"
	"github.com/stretchr/testify/assert"
	"reflect"
	"testing"
	"time"
)

func Test_decode(t *testing.T) {
	testCases := []struct{
		name string
		wantVal any
		wantErr error
		typ reflect.Type
		bs func() *sql.RawBytes
	} {
		{
			name: "nil",
			bs: func() *sql.RawBytes {
				return nil
			},
		},
		{
			name: "bool",
			bs: func() *sql.RawBytes {
				bs := sql.RawBytes("true")
				return &bs
			},
			typ: reflect.TypeOf(true),
			wantVal: true,
		},
		{
			name: "bool ptr",
			bs: func() *sql.RawBytes {
				bs := sql.RawBytes("true")
				return &bs
			},
			typ: reflect.TypeOf(ekit.ToPtr[bool](true)),
			wantVal: ekit.ToPtr[bool](true),
		},
		{
			name: "int",
			bs: func() *sql.RawBytes {
				bs := sql.RawBytes("10")
				return &bs
			},
			typ: reflect.TypeOf(10),
			wantVal: 10,
		},
		{
			name: "int ptr",
			bs: func() *sql.RawBytes {
				bs := sql.RawBytes("10")
				return &bs
			},
			typ: reflect.TypeOf(ekit.ToPtr[int](10)),
			wantVal: ekit.ToPtr[int](10),
		},
		{
			name: "int8",
			bs: func() *sql.RawBytes {
				bs := sql.RawBytes("10")
				return &bs
			},
			typ: reflect.TypeOf(int8(10)),
			wantVal: int8(10),
		},
		{
			name: "int8 ptr",
			bs: func() *sql.RawBytes {
				bs := sql.RawBytes("10")
				return &bs
			},
			typ: reflect.TypeOf(ekit.ToPtr[int8](10)),
			wantVal: ekit.ToPtr[int8](10),
		},
		{
			name: "int16",
			bs: func() *sql.RawBytes {
				bs := sql.RawBytes("10")
				return &bs
			},
			typ: reflect.TypeOf(int16(10)),
			wantVal: int16(10),
		},
		{
			name: "int16 ptr",
			bs: func() *sql.RawBytes {
				bs := sql.RawBytes("10")
				return &bs
			},
			typ: reflect.TypeOf(ekit.ToPtr[int16](10)),
			wantVal: ekit.ToPtr[int16](10),
		},
		{
			name: "int32",
			bs: func() *sql.RawBytes {
				bs := sql.RawBytes("10")
				return &bs
			},
			typ: reflect.TypeOf(int32(10)),
			wantVal: int32(10),
		},
		{
			name: "int32 ptr",
			bs: func() *sql.RawBytes {
				bs := sql.RawBytes("10")
				return &bs
			},
			typ: reflect.TypeOf(ekit.ToPtr[int32](10)),
			wantVal: ekit.ToPtr[int32](10),
		},
		{
			name: "int64",
			bs: func() *sql.RawBytes {
				bs := sql.RawBytes("10")
				return &bs
			},
			typ: reflect.TypeOf(int64(10)),
			wantVal: int64(10),
		},
		{
			name: "int64 ptr",
			bs: func() *sql.RawBytes {
				bs := sql.RawBytes("10")
				return &bs
			},
			typ: reflect.TypeOf(ekit.ToPtr[int64](10)),
			wantVal: ekit.ToPtr[int64](10),
		},
		{
			name: "uint",
			bs: func() *sql.RawBytes {
				bs := sql.RawBytes("10")
				return &bs
			},
			typ: reflect.TypeOf(uint(10)),
			wantVal: uint(10),
		},
		{
			name: "uint ptr",
			bs: func() *sql.RawBytes {
				bs := sql.RawBytes("10")
				return &bs
			},
			typ: reflect.TypeOf(ekit.ToPtr[uint](10)),
			wantVal: ekit.ToPtr[uint](10),
		},
		{
			name: "uint8",
			bs: func() *sql.RawBytes {
				bs := sql.RawBytes("10")
				return &bs
			},
			typ: reflect.TypeOf(uint8(10)),
			wantVal: uint8(10),
		},
		{
			name: "uint8 ptr",
			bs: func() *sql.RawBytes {
				bs := sql.RawBytes("10")
				return &bs
			},
			typ: reflect.TypeOf(ekit.ToPtr[uint8](10)),
			wantVal: ekit.ToPtr[uint8](10),
		},
		{
			name: "uint16",
			bs: func() *sql.RawBytes {
				bs := sql.RawBytes("10")
				return &bs
			},
			typ: reflect.TypeOf(uint16(10)),
			wantVal: uint16(10),
		},
		{
			name: "uint16 ptr",
			bs: func() *sql.RawBytes {
				bs := sql.RawBytes("10")
				return &bs
			},
			typ: reflect.TypeOf(ekit.ToPtr[uint16](10)),
			wantVal: ekit.ToPtr[uint16](10),
		},
		{
			name: "uint32",
			bs: func() *sql.RawBytes {
				bs := sql.RawBytes("10")
				return &bs
			},
			typ: reflect.TypeOf(uint32(10)),
			wantVal: uint32(10),
		},
		{
			name: "uint8 ptr",
			bs: func() *sql.RawBytes {
				bs := sql.RawBytes("10")
				return &bs
			},
			typ: reflect.TypeOf(ekit.ToPtr[uint32](10)),
			wantVal: ekit.ToPtr[uint32](10),
		},
		{
			name: "uint64",
			bs: func() *sql.RawBytes {
				bs := sql.RawBytes("10")
				return &bs
			},
			typ: reflect.TypeOf(uint64(10)),
			wantVal: uint64(10),
		},
		{
			name: "uint64 ptr",
			bs: func() *sql.RawBytes {
				bs := sql.RawBytes("10")
				return &bs
			},
			typ: reflect.TypeOf(ekit.ToPtr[uint64](10)),
			wantVal: ekit.ToPtr[uint64](10),
		},
		{
			name: "float32",
			bs: func() *sql.RawBytes {
				bs := sql.RawBytes("3.2")
				return &bs
			},
			typ: reflect.TypeOf(float32(3.2)),
			wantVal: float32(3.2),
		},
		{
			name: "float32 ptr",
			bs: func() *sql.RawBytes {
				bs := sql.RawBytes("3.2")
				return &bs
			},
			typ: reflect.TypeOf(ekit.ToPtr[float32](3.2)),
			wantVal: ekit.ToPtr[float32](3.2),
		},
		{
			name: "float64",
			bs: func() *sql.RawBytes {
				bs := sql.RawBytes("6.4")
				return &bs
			},
			typ: reflect.TypeOf(6.4),
			wantVal: 6.4,
		},
		{
			name: "float64 ptr",
			bs: func() *sql.RawBytes {
				bs := sql.RawBytes("6.4")
				return &bs
			},
			typ: reflect.TypeOf(ekit.ToPtr[float64](6.4)),
			wantVal: ekit.ToPtr[float64](6.4),
		},
		{
			name: "string",
			bs: func() *sql.RawBytes {
				bs := sql.RawBytes("abc")
				return &bs
			},
			typ: reflect.TypeOf(""),
			wantVal: "abc",
		},
		{
			name: "bytes",
			bs: func() *sql.RawBytes {
				bs := sql.RawBytes("abc")
				return &bs
			},
			typ: reflect.TypeOf([]byte("")),
			wantVal: []byte("abc"),
		},
		{
			name: "invalid type",
			bs: func() *sql.RawBytes {
				bs := sql.RawBytes("6.4")
				return &bs
			},
			typ: reflect.TypeOf(time.Now()),
			wantErr: errs.NewUnsupportedTypeError(reflect.TypeOf(time.Now())),
		},
		{
			name: "invalid ptr type",
			bs: func() *sql.RawBytes {
				bs := sql.RawBytes("6.4")
				return &bs
			},
			typ: reflect.TypeOf(ekit.ToPtr[time.Time](time.Now())),
			wantErr: errs.NewUnsupportedTypeError(reflect.TypeOf(ekit.ToPtr[time.Time](time.Now()))),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			bsPtr := tc.bs()
			res, err := decode(tc.typ, bsPtr)
			assert.Equal(t, tc.wantErr, err)
			if err != nil {
				return
			}
			assert.Equal(t, tc.wantVal, res)
		})
	}
}

func Test_decodeBool(t *testing.T) {
	bs := sql.RawBytes("true")
	res, err := decodeBool(&bs)
	assert.Nil(t, err)
	assert.True(t, res)
	bs[0]='a'
	assert.True(t, res)
	bs = sql.RawBytes("arue")
	_, err = decodeBool(&bs)
	assert.NotNil(t, err)
}

func Test_decodeInt(t *testing.T) {
	bs := sql.RawBytes("10")
	res, err := decodeInt(&bs)
	assert.Nil(t, err)
	assert.Equal(t, 10, res)
	bs[0]='a'
	assert.Equal(t, 10, res)
	bs = sql.RawBytes("a0")
	_, err = decodeInt(&bs)
	assert.NotNil(t, err)
}

func Test_decodeInt8(t *testing.T) {
	bs := sql.RawBytes("10")
	res, err := decodeInt8(&bs)
	assert.Nil(t, err)
	assert.Equal(t, int8(10), res)
	bs[0]='a'
	assert.Equal(t, int8(10), res)
	bs = sql.RawBytes("a0")
	_, err = decodeInt8(&bs)
	assert.NotNil(t, err)
}

func Test_decodeInt16(t *testing.T) {
	bs := sql.RawBytes("10")
	res, err := decodeInt16(&bs)
	assert.Nil(t, err)
	assert.Equal(t, int16(10), res)
	bs[0]='a'
	assert.Equal(t, int16(10), res)
	bs = sql.RawBytes("a0")
	_, err = decodeInt16(&bs)
	assert.NotNil(t, err)
}

func Test_decodeInt32(t *testing.T) {
	bs := sql.RawBytes("10")
	res, err := decodeInt32(&bs)
	assert.Nil(t, err)
	assert.Equal(t, int32(10), res)
	bs[0]='a'
	assert.Equal(t, int32(10), res)
	bs = sql.RawBytes("a0")
	_, err = decodeInt32(&bs)
	assert.NotNil(t, err)
}

func Test_decodeInt64(t *testing.T) {
	bs := sql.RawBytes("10")
	res, err := decodeInt64(&bs)
	assert.Nil(t, err)
	assert.Equal(t, int64(10), res)
	bs[0]='a'
	assert.Equal(t, int64(10), res)
	bs = sql.RawBytes("a0")
	_, err = decodeInt64(&bs)
	assert.NotNil(t, err)
}

func Test_decodeUint(t *testing.T) {
	bs := sql.RawBytes("10")
	res, err := decodeUint(&bs)
	assert.Nil(t, err)
	assert.Equal(t, uint(10), res)
	bs[0]='a'
	assert.Equal(t, uint(10), res)
	bs = sql.RawBytes("a0")
	_, err = decodeUint(&bs)
	assert.NotNil(t, err)
}

func Test_decodeUint8(t *testing.T) {
	bs := sql.RawBytes("10")
	res, err := decodeUint8(&bs)
	assert.Nil(t, err)
	assert.Equal(t, uint8(10), res)
	bs[0]='a'
	assert.Equal(t, uint8(10), res)
	bs = sql.RawBytes("a0")
	_, err = decodeUint8(&bs)
	assert.NotNil(t, err)
}

func Test_decodeUint16(t *testing.T) {
	bs := sql.RawBytes("10")
	res, err := decodeUint16(&bs)
	assert.Nil(t, err)
	assert.Equal(t, uint16(10), res)
	bs[0]='a'
	assert.Equal(t, uint16(10), res)
	bs = sql.RawBytes("a0")
	_, err = decodeUint16(&bs)
	assert.NotNil(t, err)
}

func Test_decodeUint32(t *testing.T) {
	bs := sql.RawBytes("10")
	res, err := decodeUint32(&bs)
	assert.Nil(t, err)
	assert.Equal(t, uint32(10), res)
	bs[0]='a'
	assert.Equal(t, uint32(10), res)
	bs = sql.RawBytes("a0")
	_, err = decodeUint32(&bs)
	assert.NotNil(t, err)
}

func Test_decodeUint64(t *testing.T) {
	bs := sql.RawBytes("10")
	res, err := decodeUint64(&bs)
	assert.Nil(t, err)
	assert.Equal(t, uint64(10), res)
	bs[0]='a'
	assert.Equal(t, uint64(10), res)
	bs = sql.RawBytes("a0")
	_, err = decodeUint64(&bs)
	assert.NotNil(t, err)
}

func Test_decodeFloat32(t *testing.T) {
	bs := sql.RawBytes("3.2")
	res, err := decodeFloat32(&bs)
	assert.Nil(t, err)
	assert.Equal(t, float32(3.2), res)
	bs[0]='a'
	assert.Equal(t, float32(3.2), res)
	bs = sql.RawBytes("a0")
	_, err = decodeFloat32(&bs)
	assert.NotNil(t, err)
}

func Test_decodeFloat64(t *testing.T) {
	bs := sql.RawBytes("6.4")
	res, err := decodeFloat64(&bs)
	assert.Nil(t, err)
	assert.Equal(t, 6.4, res)
	bs[0]='a'
	assert.Equal(t, 6.4, res)
	bs = sql.RawBytes("a0")
	_, err = decodeFloat64(&bs)
	assert.NotNil(t, err)
}


func Test_decodeString(t *testing.T) {
	bs := sql.RawBytes("hello")
	res, err := decodeString(&bs)
	assert.Nil(t, err)
	assert.Equal(t, "hello", res)
	bs[0]='a'
	assert.Equal(t, "hello", res)
}

func Test_decodeSlice(t *testing.T) {
	bs := sql.RawBytes("hello")
	res, err := decodeSlice(&bs)
	assert.Nil(t, err)
	assert.Equal(t, []byte("hello"), res)
	bs[0]='a'
	assert.Equal(t, []byte("hello"), res)
}

func Test_decodeScanner(t *testing.T) {
	typ := reflect.TypeOf(&sql.NullString{})
	testCases := []struct{
		name string
		bs *sql.RawBytes
		wantVal *sql.NullString
		wantErr error
	} {
		{
			name: "nil",
		},
		{
			name: "bs nil",
			bs: (*sql.RawBytes)(nil),
		},
		{
			name: "happy case",
			bs: ekit.ToPtr[sql.RawBytes](sql.RawBytes("a")),
			wantVal: &sql.NullString{String: "a", Valid: true},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			res, err := decodeScanner(typ, tc.bs)
			assert.Equal(t, tc.wantErr, err)
			if err != nil {
				return
			}
			assert.Equal(t, tc.wantVal, res.Interface())
		})
	}
}