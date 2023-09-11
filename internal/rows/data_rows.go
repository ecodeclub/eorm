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

	"github.com/ecodeclub/eorm/internal/errs"
)

var _ Rows = (*DataRows)(nil)

// DataRows 直接传入数据，伪装成了一个 Rows
// 非线程安全实现
type DataRows struct {
	data        [][]any
	len         int
	columns     []string
	columnTypes []*sql.ColumnType
	// 第几行
	idx int
}

func (*DataRows) NextResultSet() bool {
	return false
}

func (d *DataRows) ColumnTypes() ([]*sql.ColumnType, error) {
	return d.columnTypes, nil
}

func NewDataRows(data [][]any, columns []string, columnTypes []*sql.ColumnType) *DataRows {
	// 这里并没有什么必要检查 data 和 columns 的输入
	// 因为只有在很故意的情况下，data 和 columns 才可能会有问题
	return &DataRows{
		data:        data,
		len:         len(data),
		columns:     columns,
		idx:         -1,
		columnTypes: columnTypes,
	}
}

func (d *DataRows) Next() bool {
	if d.idx >= d.len-1 {
		return false
	}
	d.idx++
	return true
}

func (d *DataRows) Scan(dest ...any) error {
	// 不需要检测，作为内部代码我们可以预期用户会主动控制
	data := d.data[d.idx]
	if len(data) != len(dest) {
		return errs.NewErrScanWrongDestinationArguments(len(data), len(dest))
	}
	for idx, dst := range dest {
		if err := ConvertAssign(dst, data[idx]); err != nil {
			return err
		}
	}
	return nil
}

func (*DataRows) Close() error {
	return nil
}

func (d *DataRows) Columns() ([]string, error) {
	return d.columns, nil
}

func (*DataRows) Err() error {
	return nil
}
