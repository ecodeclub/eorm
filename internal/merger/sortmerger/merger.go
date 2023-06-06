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

package sortmerger

import (
	"container/heap"
	"context"
	"database/sql"
	"reflect"
	"sync"

	"go.uber.org/multierr"

	"github.com/ecodeclub/eorm/internal/merger"

	"github.com/ecodeclub/eorm/internal/merger/utils"

	"github.com/ecodeclub/eorm/internal/merger/internal/errs"
)

type Ordered interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64 | ~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 | ~float32 | ~float64 | ~string
}

type SortColumn struct {
	name  string
	order utils.Order
}

func NewSortColumn(colName string, order utils.Order) SortColumn {
	return SortColumn{
		name:  colName,
		order: order,
	}
}

type SortColumns struct {
	columns []SortColumn
	colMap  map[string]int
}

func (s SortColumns) Has(name string) bool {
	_, ok := s.colMap[name]
	return ok
}

func (s SortColumns) Find(name string) int {
	return s.colMap[name]
}

func (s SortColumns) Get(index int) SortColumn {
	return s.columns[index]
}

func (s SortColumns) Len() int {
	return len(s.columns)
}
func (s SortColumns) Cols() []SortColumn {
	return s.columns
}

// Merger  如果有GroupBy子句，会导致排序是给每个分组排的，那么该实现无法运作正常
type Merger struct {
	SortColumns
	cols []string
}

func NewMerger(sortCols ...SortColumn) (*Merger, error) {
	scs, err := newSortColumns(sortCols...)
	if err != nil {
		return nil, err
	}
	return &Merger{
		SortColumns: scs,
	}, nil
}

func newSortColumns(sortCols ...SortColumn) (SortColumns, error) {
	if len(sortCols) == 0 {
		return SortColumns{}, errs.ErrEmptySortColumns
	}
	sortMap := make(map[string]int, len(sortCols))
	for idx, sortCol := range sortCols {
		if _, ok := sortMap[sortCol.name]; ok {
			return SortColumns{}, errs.NewRepeatSortColumn(sortCol.name)
		}
		sortMap[sortCol.name] = idx
	}
	scs := SortColumns{
		columns: sortCols,
		colMap:  sortMap,
	}
	return scs, nil
}

func (m *Merger) Merge(ctx context.Context, results []*sql.Rows) (merger.Rows, error) {
	// 检测results是否符合条件
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if len(results) == 0 {
		return nil, errs.ErrMergerEmptyRows
	}
	for i := 0; i < len(results); i++ {
		val, err := checkColumns(results[i], m.cols, m.SortColumns)
		if err != nil {
			return nil, err
		}
		m.cols = val
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
	}
	return m.initRows(results)
}

func (m *Merger) initRows(results []*sql.Rows) (*Rows, error) {
	rs := &Rows{
		rowsList:    results,
		sortColumns: m.SortColumns,
		mu:          &sync.RWMutex{},
		columns:     m.cols,
	}
	h := &Heap{
		h:           make([]*node, 0, len(rs.rowsList)),
		sortColumns: rs.sortColumns,
	}
	rs.hp = h
	for i := 0; i < len(rs.rowsList); i++ {
		err := rs.nextRows(rs.rowsList[i], i)
		if err != nil {
			_ = rs.Close()
			return nil, err
		}
	}
	return rs, nil
}

func checkColumns(rows *sql.Rows, columns []string, sortCols SortColumns) ([]string, error) {
	if rows == nil {
		return nil, errs.ErrMergerRowsIsNull
	}
	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	colMap := make(map[string]struct{}, len(cols))
	if len(columns) == 0 {
		columns = cols
	}
	if len(columns) != len(cols) {
		return nil, errs.ErrMergerRowsDiff
	}
	for idx, colName := range cols {
		if columns[idx] != colName {
			return nil, errs.ErrMergerRowsDiff
		}
		colMap[colName] = struct{}{}
	}

	for _, sortColumn := range sortCols.columns {
		_, ok := colMap[sortColumn.name]
		if !ok {
			return nil, errs.NewInvalidSortColumn(sortColumn.name)
		}
	}
	return columns, nil
}

type Rows struct {
	rowsList    []*sql.Rows
	sortColumns SortColumns
	hp          *Heap
	cur         *node
	mu          *sync.RWMutex
	lastErr     error
	closed      bool
	columns     []string
}

func (r *Rows) Next() bool {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return false
	}
	if r.hp.Len() == 0 || r.lastErr != nil {
		r.mu.Unlock()
		_ = r.Close()
		return false
	}
	r.cur = heap.Pop(r.hp).(*node)
	row := r.rowsList[r.cur.index]
	err := r.nextRows(row, r.cur.index)
	if err != nil {
		r.lastErr = err
		r.mu.Unlock()
		_ = r.Close()
		return false
	}
	r.mu.Unlock()
	return true
}

func (r *Rows) nextRows(row *sql.Rows, index int) error {
	if row.Next() {
		n, err := newNode(row, r.sortColumns, index)
		if err != nil {
			return err
		}
		heap.Push(r.hp, n)
	} else if row.Err() != nil {
		return row.Err()
	}
	return nil
}
func newNode(row *sql.Rows, sortCols SortColumns, index int) (*node, error) {
	colsInfo, err := row.ColumnTypes()
	if err != nil {
		return nil, err
	}
	columns := make([]any, 0, len(colsInfo))
	sortColumns := make([]any, sortCols.Len())
	for _, colInfo := range colsInfo {
		colName := colInfo.Name()
		colType := colInfo.ScanType()
		for colType.Kind() == reflect.Ptr {
			colType = colType.Elem()
		}
		column := reflect.New(colType).Interface()
		if sortCols.Has(colName) {
			sortIndex := sortCols.Find(colName)
			sortColumns[sortIndex] = column
		}
		columns = append(columns, column)
	}
	err = row.Scan(columns...)
	if err != nil {
		return nil, err
	}
	for i := 0; i < len(sortColumns); i++ {
		sortColumns[i] = reflect.ValueOf(sortColumns[i]).Elem().Interface()
	}
	for i := 0; i < len(columns); i++ {
		columns[i] = reflect.ValueOf(columns[i]).Elem().Interface()
	}
	return &node{
		sortCols: sortColumns,
		columns:  columns,
		index:    index,
	}, nil
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
	if r.cur == nil {
		return errs.ErrMergerScanNotNext
	}
	var err error
	for i := 0; i < len(dest); i++ {
		err = utils.ConvertAssign(dest[i], r.cur.columns[i])
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

func (r *Rows) Err() error {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.lastErr
}

func (r *Rows) Columns() ([]string, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.closed {
		return nil, errs.ErrMergerRowsClosed
	}
	return r.columns, nil
}
