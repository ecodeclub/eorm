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
	_ "unsafe"

	"go.uber.org/multierr"

	"github.com/ecodeclub/eorm/internal/merger"
	"github.com/ecodeclub/eorm/internal/merger/internal/errs"
)

type Order bool

const (
	// ASC 升序排序
	ASC Order = true
	// DESC 降序排序
	DESC Order = false
)

type Ordered interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64 | ~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 | ~float32 | ~float64 | ~string
}

//go:linkname convertAssign database/sql.convertAssign
func convertAssign(dest, src any) error

type SortColumn struct {
	name  string
	order Order
	typ   reflect.Type
}

func NewSortColumn[T Ordered](colName string, order Order) SortColumn {
	var t T
	typ := reflect.TypeOf(t)
	return SortColumn{
		name:  colName,
		order: order,
		typ:   typ,
	}
}

type sortColumns struct {
	columns []SortColumn
	colMap  map[string]int
}

func (s sortColumns) Has(name string) bool {
	_, ok := s.colMap[name]
	return ok
}

func (s sortColumns) Find(name string) (SortColumn, int) {
	return s.columns[s.colMap[name]], s.colMap[name]
}

func (s sortColumns) Get(index int) SortColumn {
	return s.columns[index]
}

func (s sortColumns) Len() int {
	return len(s.columns)
}

// Merger  如果有 GroupBy 子句，那么该实现无法运作正常
type Merger struct {
	sortColumns
	cols []string
}

func NewMerger(sortCols ...SortColumn) (*Merger, error) {
	scs, err := newSortColumns(sortCols...)
	if err != nil {
		return nil, err
	}
	return &Merger{
		sortColumns: scs,
	}, nil
}

func newSortColumns(sortCols ...SortColumn) (sortColumns, error) {
	if len(sortCols) == 0 {
		return sortColumns{}, errs.ErrEmptySortColumns
	}
	sortMap := make(map[string]int, len(sortCols))
	for idx, sortCol := range sortCols {
		if _, ok := sortMap[sortCol.name]; ok {
			return sortColumns{}, errs.NewRepeatSortColumn(sortCol.name)
		}
		sortMap[sortCol.name] = idx
	}
	scs := sortColumns{
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
		if err := m.checkColumns(results[i]); err != nil {
			return nil, err
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
	}
	return m.initRows(results)
}

func (m *Merger) initRows(results []*sql.Rows) (*Rows, error) {
	rows := &Rows{
		rowsList:    results,
		sortColumns: m.sortColumns,
		mu:          &sync.RWMutex{},
		columns:     m.cols,
	}
	h := &Heap{
		h:           make([]*node, 0, len(rows.rowsList)),
		sortColumns: rows.sortColumns,
	}
	rows.hp = h
	for i := 0; i < len(rows.rowsList); i++ {
		err := rows.nextRows(rows.rowsList[i], i)
		if err != nil {
			_ = rows.Close()
			return nil, err
		}
	}
	return rows, nil
}

func (m *Merger) checkColumns(rows *sql.Rows) error {
	if rows == nil {
		return errs.ErrMergerRowsIsNull
	}
	cols, err := rows.Columns()
	if err != nil {
		return err
	}
	colMap := make(map[string]struct{}, len(cols))
	if len(m.cols) == 0 {
		m.cols = cols
	}
	if len(m.cols) != len(cols) {
		return errs.ErrMergerRowsDiff
	}
	for idx, colName := range cols {
		if m.cols[idx] != colName {
			return errs.ErrMergerRowsDiff
		}
		colMap[colName] = struct{}{}
	}

	for _, sortColumn := range m.sortColumns.columns {
		_, ok := colMap[sortColumn.name]
		if !ok {
			return errs.NewInvalidSortColumn(sortColumn.name)
		}
	}
	return nil
}

func newNode(row *sql.Rows, sortCols sortColumns, index int) (*node, error) {
	colsInfo, err := row.ColumnTypes()
	if err != nil {
		return nil, err
	}
	columns := make([]any, 0, len(colsInfo))
	sortColumns := make([]any, sortCols.Len())
	for _, colInfo := range colsInfo {
		colName := colInfo.Name()
		if sortCols.Has(colName) {
			sortCol, sortIndex := sortCols.Find(colName)
			sortColumn := reflect.New(sortCol.typ).Interface()
			sortColumns[sortIndex] = sortColumn
			columns = append(columns, sortColumn)
		} else {
			columns = append(columns, &[]byte{})
		}
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

type Rows struct {
	rowsList    []*sql.Rows
	sortColumns sortColumns
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

	for i := 0; i < len(dest); i++ {
		err := convertAssign(dest[i], r.cur.columns[i])
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
