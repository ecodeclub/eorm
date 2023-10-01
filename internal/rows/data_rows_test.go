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
	"errors"
	"testing"

	"github.com/ecodeclub/ekit"
	"github.com/ecodeclub/eorm/internal/errs"
	"github.com/stretchr/testify/assert"
)

func TestDataRows_Close(t *testing.T) {
	rows := NewDataRows(nil, nil, nil)
	assert.Nil(t, rows.Close())
}

func TestDataRows_Columns(t *testing.T) {
	testCases := []struct {
		name    string
		columns []string
	}{
		{
			name: "nil",
		},
		{
			name:    "columns",
			columns: []string{"column1"},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			rows := NewDataRows(nil, tc.columns, nil)
			columns, err := rows.Columns()
			assert.NoError(t, err)
			assert.Equal(t, tc.columns, columns)
		})
	}
}

func TestDataRows_Err(t *testing.T) {
	rows := NewDataRows(nil, nil, nil)
	assert.NoError(t, rows.Err())
}

func TestDataRows_Next(t *testing.T) {
	testCases := []struct {
		name      string
		data      [][]any
		beforeIdx int

		wantNext bool
		afterIdx int
	}{
		{
			name:      "nil",
			wantNext:  false,
			beforeIdx: -1,
			afterIdx:  -1,
		},
		{
			name:      "第一个",
			data:      [][]any{{1, 2, 3}},
			wantNext:  true,
			beforeIdx: -1,
			afterIdx:  0,
		},
		{
			name:      "还有一个",
			data:      [][]any{{1}, {2}},
			beforeIdx: 0,
			wantNext:  true,
			afterIdx:  1,
		},
		{
			name:      "到了最后一个",
			data:      [][]any{{1}, {2}},
			beforeIdx: 1,
			wantNext:  false,
			afterIdx:  1,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			rows := NewDataRows(tc.data, nil, nil)
			rows.idx = tc.beforeIdx
			assert.Equal(t, tc.wantNext, rows.Next())
			assert.Equal(t, tc.afterIdx, rows.idx)
		})
	}
}

func TestDataRows_Scan(t *testing.T) {
	testCases := []struct {
		name string
		data [][]any
		idx  int

		input   []any
		wantRes []any
		wantErr error
	}{
		{
			name:  "获得了数据",
			data:  [][]any{{1, 2, 3}},
			input: []any{new(int), new(int32), new(int64)},
			wantRes: []any{ekit.ToPtr[int](1),
				ekit.ToPtr[int32](2), ekit.ToPtr[int64](3)},
			wantErr: nil,
		},
		{
			name:    "dst 过长",
			data:    [][]any{{1, 2, 3}},
			input:   []any{new(int), new(int32), new(int64), new(int64)},
			wantErr: errs.NewErrScanWrongDestinationArguments(3, 4),
		},
		{
			name:    "dst 过短",
			data:    [][]any{{1, 2, 3}},
			input:   []any{new(int), new(int32)},
			wantErr: errs.NewErrScanWrongDestinationArguments(3, 2),
		},
		{
			name:    "ConvertAndAssign错误",
			data:    [][]any{{1, "abc", 3}},
			input:   []any{new(int), new(int64), new(int64)},
			wantErr: errors.New(`converting driver.Value type string ("abc") to a int64: invalid syntax`),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			rows := NewDataRows(tc.data, nil, nil)
			rows.idx = tc.idx
			err := rows.Scan(tc.input...)
			assert.Equal(t, tc.wantErr, err)
			if err != nil {
				return
			}
			assert.Equal(t, tc.wantRes, tc.input)
		})
	}
}

func TestDataRows_NextResultSet(t *testing.T) {
	// 固化行为，防止不小心改了
	rows := NewDataRows(nil, nil, nil)
	assert.False(t, rows.NextResultSet())
}
