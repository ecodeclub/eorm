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

package rows

import (
	"database/sql"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConvertNullable(t *testing.T) {
	testcases := []struct {
		name    string
		src     any
		dest    any
		wantVal any
		hasErr  bool
	}{
		{
			name:    "sql.NUllbool",
			src:     sql.NullBool{Valid: true, Bool: true},
			dest:    &sql.NullBool{Valid: false, Bool: false},
			wantVal: &sql.NullBool{Valid: true, Bool: true},
		},
		{
			name:    "sql.NUllbool的valid为false",
			src:     sql.NullBool{Valid: false, Bool: true},
			dest:    &sql.NullBool{Valid: false, Bool: false},
			wantVal: &sql.NullBool{Valid: false, Bool: false},
		},
		{
			name:    "sql.NUllString",
			src:     sql.NullString{Valid: true, String: "xx"},
			dest:    &sql.NullString{Valid: false, String: ""},
			wantVal: &sql.NullString{Valid: true, String: "xx"},
		},
		{
			name:    "sql.NUllString的valid为false",
			src:     sql.NullString{Valid: false, String: "xx"},
			dest:    &sql.NullString{Valid: false, String: ""},
			wantVal: &sql.NullString{Valid: false, String: ""},
		},
		{
			name:    "sql.NUllByte",
			src:     sql.NullByte{Valid: true, Byte: 'a'},
			dest:    &sql.NullByte{Valid: false, Byte: ' '},
			wantVal: &sql.NullByte{Valid: true, Byte: 'a'},
		},
		{
			name:    "sql.NUllByte的valid的false",
			src:     sql.NullByte{Valid: false, Byte: 'a'},
			dest:    &sql.NullByte{Valid: false, Byte: 0},
			wantVal: &sql.NullByte{Valid: false, Byte: 0},
		},
		{
			name:    "sql.NUllInt32",
			src:     sql.NullInt32{Valid: true, Int32: 5},
			dest:    &sql.NullInt32{Valid: false, Int32: 0},
			wantVal: &sql.NullInt32{Valid: true, Int32: 5},
		},
		{
			name:    "sql.NUllInt32的valid的false",
			src:     sql.NullInt32{Valid: false, Int32: 0},
			dest:    &sql.NullInt32{Valid: false, Int32: 0},
			wantVal: &sql.NullInt32{Valid: false, Int32: 0},
		},
		{
			name:    "sql.NUllInt64",
			src:     sql.NullInt64{Valid: true, Int64: 5},
			dest:    &sql.NullInt64{Valid: false, Int64: 0},
			wantVal: &sql.NullInt64{Valid: true, Int64: 5},
		},
		{
			name:    "sql.NUllInt64的valid的false",
			src:     sql.NullInt64{Valid: false, Int64: 0},
			dest:    &sql.NullInt64{Valid: false, Int64: 0},
			wantVal: &sql.NullInt64{Valid: false, Int64: 0},
		},
		{
			name:    "sql.NUllInt16",
			src:     sql.NullInt16{Valid: true, Int16: 5},
			dest:    &sql.NullInt16{Valid: false, Int16: 0},
			wantVal: &sql.NullInt16{Valid: true, Int16: 5},
		},
		{
			name:    "sql.NUllInt16的valid的false",
			src:     sql.NullInt16{Valid: false, Int16: 0},
			dest:    &sql.NullInt16{Valid: false, Int16: 0},
			wantVal: &sql.NullInt16{Valid: false, Int16: 0},
		},
		{
			name:    "sql.NUllFloat64",
			src:     sql.NullFloat64{Valid: true, Float64: 5},
			dest:    &sql.NullFloat64{Valid: false, Float64: 0},
			wantVal: &sql.NullFloat64{Valid: true, Float64: 5},
		},
		{
			name:    "sql.NUllfloat64的valid的false",
			src:     sql.NullFloat64{Valid: false, Float64: 0},
			dest:    &sql.NullFloat64{Valid: false, Float64: 0},
			wantVal: &sql.NullFloat64{Valid: false, Float64: 0},
		},
		{
			name: "sql.NUllTime",
			src: sql.NullTime{Valid: true, Time: func() time.Time {
				val, err := time.ParseInLocation("2006-01-02 15:04:05", "2022-01-01 12:00:00", time.Local)
				require.NoError(t, err)
				return val
			}()},
			dest: &sql.NullTime{Valid: false, Time: time.Time{}},
			wantVal: &sql.NullTime{Valid: true, Time: func() time.Time {
				val, err := time.ParseInLocation("2006-01-02 15:04:05", "2022-01-01 12:00:00", time.Local)
				require.NoError(t, err)
				return val
			}()},
		},
		{
			name:    "sql.NUllTime的valid的false",
			src:     sql.NullTime{Valid: false, Time: time.Time{}},
			dest:    &sql.NullTime{Valid: false, Time: time.Time{}},
			wantVal: &sql.NullTime{Valid: false, Time: time.Time{}},
		},
		{
			name:    "使用sql.NullInt32接收sql.NullInt64",
			src:     sql.NullInt64{Valid: true, Int64: 5},
			dest:    &sql.NullInt32{Valid: false, Int32: 0},
			wantVal: &sql.NullInt32{Valid: true, Int32: 5},
		},
		{
			name:    "使用sql.NullInt16接收sql.NullInt64",
			src:     sql.NullInt64{Valid: true, Int64: 5},
			dest:    &sql.NullInt16{Valid: false, Int16: 0},
			wantVal: &sql.NullInt16{Valid: true, Int16: 5},
		},
		{
			name:    "使用sql.NullInt32接收sql.NullInt64,Valid为false",
			src:     sql.NullInt64{Valid: false, Int64: 0},
			dest:    &sql.NullInt32{Valid: false, Int32: 0},
			wantVal: &sql.NullInt32{Valid: false, Int32: 0},
		},
		{
			name:    "使用sql.NullInt16接收sql.NullInt64,Valid为false",
			src:     sql.NullInt64{Valid: false, Int64: 0},
			dest:    &sql.NullInt16{Valid: false, Int16: 0},
			wantVal: &sql.NullInt16{Valid: false, Int16: 0},
		},
		{
			name: "使用int32接收sql.NullInt64",
			src:  sql.NullInt64{Valid: true, Int64: 5},
			dest: func() *int32 {
				var val int32
				return &val
			}(),
			wantVal: func() *int32 {
				val := int32(5)
				return &val
			}(),
		},
		{
			name: "使用int16接收sql.NullInt64",
			src:  sql.NullInt64{Valid: true, Int64: 5},
			dest: func() *int16 {
				var val int16
				return &val
			}(),
			wantVal: func() *int16 {
				val := int16(5)
				return &val
			}(),
		},
		{
			name: "使用float32接收sql.Nullfloat64",
			src:  sql.NullFloat64{Valid: true, Float64: 5},
			dest: func() *float32 {
				var val float32
				return &val
			}(),
			wantVal: func() *float32 {
				val := float32(5)
				return &val
			}(),
		},
		{
			name: "使用int32接收sql.NullInt64,Valid为false",
			src:  sql.NullInt64{Valid: false, Int64: 0},
			dest: func() *int32 {
				var val int32
				return &val
			}(),
			hasErr: true,
		},
		{
			name: "使用int16接收sql.NullInt64,valid为false",
			src:  sql.NullInt64{Valid: false, Int64: 0},
			dest: func() *int16 {
				var val int16
				return &val
			}(),
			hasErr: true,
		},
		{
			name: "使用float32接收sql.Nullfloat64",
			src:  sql.NullFloat64{Valid: false, Float64: 0},
			dest: func() *float32 {
				var val float32
				return &val
			}(),
			hasErr: true,
		},
	}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			err := ConvertAssign(tc.dest, tc.src)
			if tc.hasErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.dest, tc.wantVal)
		})
	}
}
