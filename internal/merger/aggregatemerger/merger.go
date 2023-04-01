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
	aggregatorColumns := make([][][]any, 0, len(r.rowsList))
	hasOpen := false
	hasClosed := false
	for idx, row := range r.rowsList {
		aggregatorColumn, ok, err := r.getCols(row)
		if err != nil {
			r.lastErr = err
			r.mu.Unlock()
			_ = r.Close()
			return false
		}
		if !ok && hasOpen || ok && hasClosed {
			r.lastErr = errs.ErrMergerAggregateHasEmptyRows
			r.mu.Unlock()
			_ = r.Close()
			return false
		} else if !ok && idx == len(r.rowsList)-1 {
			r.mu.Unlock()
			_ = r.Close()
			return false
		}
		if ok {
			hasOpen = true
		} else {
			hasClosed = true
		}
		aggregatorColumns = append(aggregatorColumns, aggregatorColumn)
	}
	// 进行聚合函数计算
	results := make([]any, 0, len(r.aggregators))
	for idx, agg := range r.aggregators {
		aggCols := make([][]any, 0, len(aggregatorColumns))
		for i := 0; i < len(aggregatorColumns); i++ {
			aggCols = append(aggCols, aggregatorColumns[i][idx])
		}
		res, err := agg.Aggregate(aggCols)
		if err != nil {
			r.lastErr = err
			r.mu.Unlock()
			_ = r.Close()
			return false
		}
		results = append(results, res)
	}
	r.cur = results
	r.mu.Unlock()
	return true
}

// getCols 从sqlRows里面获取数据
func (r *Rows) getCols(row *sql.Rows) ([][]any, bool, error) {
	colsData := make([][]any, 0, len(r.aggregators))
	if row.Next() {
		colMap := make(map[string][][]int)
		for idx, agg := range r.aggregators {
			infos := agg.ColumnInfo()
			col := make([]any, len(infos))
			for _, colInfo := range infos {
				newColData := reflect.New(colInfo.Typ).Interface()
				col[colInfo.Index] = newColData
				if _, ok := colMap[colInfo.Name]; !ok {
					colMap[colInfo.Name] = [][]int{[]int{idx, colInfo.Index}}
				} else {
					colMap[colInfo.Name] = append(colMap[colInfo.Name], []int{idx, colInfo.Index})
				}
			}
			colsData = append(colsData, col)
		}
		columnsInfo, err := row.ColumnTypes()
		if err != nil {
			return nil, false, err
		}
		res := make([]any, 0, len(columnsInfo))
		for _, columnInfo := range columnsInfo {
			val, ok := colMap[columnInfo.Name()]
			if !ok {
				res = append(res, &[]byte{})
				continue
			}
			res = append(res, colsData[val[0][0]][val[0][1]])
			if len(val) == 1 {
				delete(colMap, columnInfo.Name())
			} else {
				colMap[columnInfo.Name()] = colMap[columnInfo.Name()][1:]
			}
		}
		if len(colMap) != 0 {
			return nil, false, errs.ErrMergerAggregateColumnNotFound
		}
		err = row.Scan(res...)
		if err != nil {
			return nil, false, err
		}
		for i := 0; i < len(colsData); i++ {
			for j := 0; j < len(colsData[i]); j++ {
				colsData[i][j] = reflect.ValueOf(colsData[i][j]).Elem().Interface()
			}
		}
	} else {
		if row.Err() != nil {
			return nil, false, row.Err()
		}
		return nil, false, nil
	}
	return colsData, true, nil
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
