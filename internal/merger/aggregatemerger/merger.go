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

package aggregatemerger

import (
	"context"
	"database/sql"
	"reflect"
	"sync"
	_ "unsafe"

	"github.com/ecodeclub/eorm/internal/merger"
	"github.com/ecodeclub/eorm/internal/merger/aggregatemerger/aggregator"
	"github.com/ecodeclub/eorm/internal/merger/internal/errs"
	"go.uber.org/multierr"
)

//go:linkname convertAssign database/sql.convertAssign
func convertAssign(dest, src any) error

type Merger struct {
	aggregators []aggregator.Aggregator
	cols        []string
}

func NewMerger(aggregators ...aggregator.Aggregator) *Merger {
	cols := make([]string, 0, len(aggregators))
	for _, agg := range aggregators {
		cols = append(cols, agg.ColumnName())
	}
	return &Merger{
		aggregators: aggregators,
		cols:        cols,
	}
}

func (m *Merger) Merge(ctx context.Context, results []*sql.Rows) (merger.Rows, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if len(results) == 0 {
		return nil, errs.ErrMergerEmptyRows
	}
	for _, res := range results {
		err := m.checkColumns(res)
		if err != nil {
			return nil, err
		}
	}

	return &Rows{
		rowsList:    results,
		aggregators: m.aggregators,
		mu:          &sync.RWMutex{},
		columns:     m.cols,
	}, nil

}
func (m *Merger) checkColumns(rows *sql.Rows) error {
	if rows == nil {
		return errs.ErrMergerRowsIsNull
	}
	return nil
}

type Rows struct {
	rowsList    []*sql.Rows
	aggregators []aggregator.Aggregator
	closed      bool
	mu          *sync.RWMutex
	lastErr     error
	cur         []any
	columns     []string
}

func (r *Rows) Next() bool {
	r.mu.Lock()
	if r.closed || r.lastErr != nil {
		r.mu.Unlock()
		return false
	}
	// 从rowsList里面获取数据
	rowsData, ok, err := r.getColsInfo()
	if err != nil {
		r.lastErr = err
		r.mu.Unlock()
		_ = r.Close()
		return false
	}
	if !ok {
		r.mu.Unlock()
		_ = r.Close()
		return false
	}
	// 进行聚合函数计算
	res, err := r.getAggregateInfo(rowsData)
	if err != nil {
		r.lastErr = err
		r.mu.Unlock()
		_ = r.Close()
		return false
	}
	r.cur = res
	r.mu.Unlock()
	return true

}

// getAggregateInfo 进行aggregate运算
func (r *Rows) getAggregateInfo(rowsData [][]any) ([]any, error) {
	res := make([]any, 0, len(r.aggregators))
	for _, agg := range r.aggregators {
		val, err := agg.Aggregate(rowsData)
		if err != nil {
			return nil, err
		}
		res = append(res, val)
	}
	return res, nil
}

// getColInfo 从sqlRows里面获取数据
func (r *Rows) getColsInfo() ([][]any, bool, error) {
	// 所有sql.Rows的数据
	rowsData := make([][]any, 0, len(r.rowsList))
	hasClosed := false
	hasOpen := false
	// hasClosed表示前面的有sql.Rows他的Next的结果为false
	// hasOpen 表示前面有sql.Rows他的Next的结果为true
	// hasClosed和hasOpen是为了当出现有RowsList中有一个或多个sql.Rows出现返回行数为空的时候报错（全部为空不会报错）。
	for idx, row := range r.rowsList {
		colsInfo, err := row.ColumnTypes()
		if err != nil {
			return nil, false, err
		}
		// colsData 表示一个sql.Rows的数据
		colsData := make([]any, 0, len(colsInfo))
		if row.Next() {
			// 当前面有sql.Rows他的Next结果为false，并且当前的sql.Rows为true。那就会报错sql.Rows列表里面有元素返回空行
			if hasClosed {
				return nil, false, errs.ErrMergerAggregateHasEmptyRows
			}
			// 拿到sql.Rows字段的类型然后初始化
			for _, colInfo := range colsInfo {
				typ := colInfo.ScanType()
				// sqlite3的驱动返回的是指针。循环的去除指针
				for typ.Kind() == reflect.Pointer {
					typ = typ.Elem()
				}
				newData := reflect.New(typ).Interface()
				colsData = append(colsData, newData)
			}
			// 通过Scan赋值
			err = row.Scan(colsData...)
			if err != nil {
				return nil, false, err
			}
			// 去掉reflect.New的指针
			for i := 0; i < len(colsData); i++ {
				colsData[i] = reflect.ValueOf(colsData[i]).Elem().Interface()
			}

			hasOpen = true
		} else {
			// sql.Rows迭代过程中发生报错，返回报错
			if row.Err() != nil {
				return nil, false, row.Err()
			}
			// 前面有sql.Rows返回的行数非空，当前为空行返回报错
			if hasOpen {
				return nil, false, errs.ErrMergerAggregateHasEmptyRows
			}
			hasClosed = true
			// 全部sql.Rows返回都是空，说明遍历完了，或者都没有查询到数据，不返回报错
			if idx == len(r.rowsList)-1 {
				return nil, false, nil
			}

		}
		rowsData = append(rowsData, colsData)
	}
	return rowsData, true, nil
}

func (r *Rows) Scan(dest ...any) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.lastErr != nil {
		return r.lastErr
	}
	if r.closed {
		return errs.ErrMergerRowsClosed
	}

	if len(r.cur) == 0 {
		return errs.ErrMergerScanNotNext
	}
	for i := 0; i < len(dest); i++ {
		err := convertAssign(dest[i], r.cur[i])
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *Rows) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.closed = true
	errorList := make([]error, 0, len(r.rowsList))
	for i := 0; i < len(r.rowsList); i++ {
		row := r.rowsList[i]
		err := row.Close()
		if err != nil {
			errorList = append(errorList, err)
		}
	}
	return multierr.Combine(errorList...)
}

func (r *Rows) Columns() ([]string, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.closed {
		return nil, errs.ErrMergerRowsClosed
	}
	return r.columns, nil
}

func (r *Rows) Err() error {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.lastErr
}
