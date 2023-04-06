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
		aggDatas := make([][]any, 0, len(r.rowsList))
		aggInfo := agg.ColumnInfo()
		for _, rowData := range rowsData {
			aggData := make([]any, 0, len(aggInfo))
			for _, colInfo := range aggInfo {
				index := colInfo.Index
				if index >= len(rowData) || index < 0 {
					return nil, errs.ErrMergerInvalidAggregateColumnIndex
				}
				aggData = append(aggData, rowData[index])
			}
			aggDatas = append(aggDatas, aggData)
		}
		val, err := agg.Aggregate(aggDatas)
		if err != nil {
			return nil, err
		}
		res = append(res, val)
	}
	return res, nil
}

// getColInfo 从sqlRows里面获取数据
func (r *Rows) getColsInfo() ([][]any, bool, error) {
	rowsData := make([][]any, 0, len(r.rowsList))
	hasClosed := false
	hasOpen := false
	for idx, row := range r.rowsList {
		colsInfo, err := row.ColumnTypes()
		if err != nil {
			return nil, false, err
		}
		colsData := make([]any, 0, len(colsInfo))
		if row.Next() {
			if hasClosed {
				return nil, false, errs.ErrMergerAggregateHasEmptyRows
			}
			for _, colInfo := range colsInfo {
				typ := colInfo.ScanType()
				// sqlite3的驱动返回的是指针。循环的去除指针
				for typ.Kind() == reflect.Pointer {
					typ = typ.Elem()
				}
				newData := reflect.New(typ).Interface()
				colsData = append(colsData, newData)
			}
			err = row.Scan(colsData...)
			if err != nil {
				return nil, false, err
			}
			for i := 0; i < len(colsData); i++ {
				colsData[i] = reflect.ValueOf(colsData[i]).Elem().Interface()
			}
			hasOpen = true
		} else {
			if row.Err() != nil {
				return nil, false, row.Err()
			}
			if hasOpen {
				return nil, false, errs.ErrMergerAggregateHasEmptyRows
			}
			hasClosed = true
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
