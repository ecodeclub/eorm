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
	"database/sql/driver"
	"reflect"
	"sync"

	"github.com/ecodeclub/ekit/mapx"
	"github.com/ecodeclub/eorm/internal/merger"
	"github.com/ecodeclub/eorm/internal/merger/internal/errs"
	"github.com/ecodeclub/eorm/internal/merger/utils"
	"go.uber.org/multierr"
)

type DistinctMerger struct {
	sortCols        SortColumns
	hasOrderBy      bool
	distinctColumns []merger.ColumnInfo
	cols            []string
}

type key struct {
	data []any
}

func compareKey(a, b key) int {
	keyLen := len(a.data)
	for i := 0; i < keyLen; i++ {
		var cp func(any, any, utils.Order) int
		if _, ok := a.data[i].(driver.Valuer); ok {
			cp = utils.CompareNullable
		} else {
			cp = utils.CompareFuncMapping[reflect.TypeOf(a.data[i]).Kind()]
		}
		res := cp(a.data[i], b.data[i], utils.ASC)
		if res != 0 {
			return res
		}
	}
	return 0
}
func (o *DistinctMerger) Merge(ctx context.Context, results []*sql.Rows) (merger.Rows, error) {
	// 检测results是否符合条件
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if len(results) == 0 {
		return nil, errs.ErrMergerEmptyRows
	}

	for i := 0; i < len(results); i++ {
		err := o.checkColumns(results[i])
		if err != nil {
			return nil, err
		}
	}
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	return o.initRows(results)
}
func (o *DistinctMerger) checkColumns(rows *sql.Rows) error {
	if rows == nil {
		return errs.ErrMergerRowsIsNull
	}
	cols, err := rows.Columns()
	if err != nil {
		return err
	}
	// 判断数据库里的列只有去重列，且顺序要和定义的顺序一致
	if len(cols) != len(o.distinctColumns) {
		return errs.ErrDistinctColsNotInCols
	}
	for _, distinctCol := range o.distinctColumns {
		if cols[distinctCol.Index] != distinctCol.Name {
			return errs.ErrDistinctColsNotInCols
		}
	}
	if !o.hasOrderBy {
		o.cols = cols
	} else {
		o.cols, err = checkColumns(rows, cols, o.sortCols)
	}

	return err
}
func (o *DistinctMerger) initRows(results []*sql.Rows) (*DistinctRows, error) {
	h := &Heap{
		h:           make([]*node, 0, len(results)),
		sortColumns: o.sortCols,
	}
	t, err := mapx.NewTreeMap[key, struct{}](compareKey)
	if err != nil {
		return nil, err
	}
	err = o.initMapAndHeap(results, t, h)
	if err != nil {
		return nil, err
	}
	return &DistinctRows{
		distinctCols: o.distinctColumns,
		rowsList:     results,
		sortCols:     o.sortCols,
		hp:           h,
		treeMap:      t,
		mu:           &sync.RWMutex{},
		columns:      o.cols,
		hasOrderBy:   o.hasOrderBy,
	}, nil
}

type DistinctRows struct {
	distinctCols []merger.ColumnInfo
	rowsList     []*sql.Rows
	sortCols     SortColumns
	hp           *Heap
	mu           *sync.RWMutex
	treeMap      *mapx.TreeMap[key, struct{}]
	cur          []any
	closed       bool
	lastErr      error
	columns      []string
	hasOrderBy   bool
}

func (o *DistinctRows) Next() bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.closed {
		return false
	}
	if o.hp.Len() == 0 && len(o.treeMap.Keys()) == 0 || o.lastErr != nil {
		_ = o.closeRows()
		return false
	}
	val := o.treeMap.Keys()[0]
	o.cur = val.data
	// 删除当前的数据行
	_, _ = o.treeMap.Delete(val)
	// 当一个排序列的数据取完就取下一个排序列的全部数据
	if len(o.treeMap.Keys()) == 0 {
		_, err := balance(o.rowsList, o.treeMap, o.sortCols, o.hp, o.hasOrderBy)
		if err != nil {
			o.lastErr = err
			_ = o.closeRows()
			return false
		}
	}
	return true
}

// 初始化堆和map，保证至少有一个排序列相同的所有数据全部拿出。第一个返回值表示results还有没有值
func (o *DistinctMerger) initMapAndHeap(results []*sql.Rows, t *mapx.TreeMap[key, struct{}], h *Heap) error {

	// 初始化将所有sql.Rows的第一个元素塞进heap中
	for i := 0; i < len(results); i++ {
		if results[i].Next() {

			n, err := newDistinctNode(results[i], o.sortCols, i, o.hasOrderBy)
			if err != nil {
				return err
			}
			heap.Push(h, n)
		} else if results[i].Err() != nil {
			return results[i].Err()
		}
	}
	// 如果四个results里面的元素均为空表示没有已经没有数据了

	_, err := balance(results, t, o.sortCols, h, o.hasOrderBy)
	return err
}

func newDistinctNode(rows *sql.Rows, sortCols SortColumns, index int, hasOrderBy bool) (*node, error) {
	n, err := newNode(rows, sortCols, index)
	if err != nil {
		return nil, err
	}
	if !hasOrderBy {
		n.sortCols = []any{1}
	}
	return n, nil
}

// 从heap中取出一个排序列的所有行，保存进treemap中
func balance(results []*sql.Rows, t *mapx.TreeMap[key, struct{}], sortCols SortColumns, h *Heap, hasOrderBy bool) (bool, error) {
	var sortCol []any
	if h.Len() == 0 {
		return false, nil
	}
	for i := 0; ; i++ {
		if h.Len() == 0 {
			return false, nil
		}
		val := heap.Pop(h).(*node)
		if i == 0 {
			sortCol = val.sortCols
		}
		// 相同元素进入treemap
		if compareKey(key{val.sortCols}, key{sortCol}) == 0 {
			err := t.Put(key{val.columns}, struct{}{})
			if err != nil {
				return false, err
			}
			// 将后续元素加入heap
			r := results[val.index]
			if r.Next() {
				n, err := newDistinctNode(r, sortCols, val.index, hasOrderBy)
				if err != nil {
					return false, err
				}
				heap.Push(h, n)
			} else if r.Err() != nil {
				return false, r.Err()
			}
		} else {
			// 如果排序列不相同将 拿出来的元素，重新塞进heap中
			heap.Push(h, val)
			return true, nil
		}

	}
}

func (r *DistinctRows) Scan(dest ...any) error {
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
		err = utils.ConvertAssign(dest[i], r.cur[i])
		if err != nil {
			return err
		}
	}
	return nil
}

func (o *DistinctRows) Close() error {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.closed = true
	errorList := make([]error, 0, len(o.rowsList))
	for i := 0; i < len(o.rowsList); i++ {
		row := o.rowsList[i]
		err := row.Close()
		if err != nil {
			errorList = append(errorList, err)
		}
	}
	return multierr.Combine(errorList...)
}

func (o *DistinctRows) closeRows() error {
	o.closed = true
	errorList := make([]error, 0, len(o.rowsList))
	for i := 0; i < len(o.rowsList); i++ {
		row := o.rowsList[i]
		err := row.Close()
		if err != nil {
			errorList = append(errorList, err)
		}
	}
	return multierr.Combine(errorList...)
}

func (o *DistinctRows) Columns() ([]string, error) {
	o.mu.RLock()
	defer o.mu.RUnlock()
	if o.closed {
		return nil, errs.ErrMergerRowsClosed
	}
	return o.columns, nil
}

func (o *DistinctRows) Err() error {
	o.mu.RLock()
	defer o.mu.RUnlock()
	return o.lastErr
}

func NewDistinctMerger(distinctCols []merger.ColumnInfo, sortColList *SortColumns) (*DistinctMerger, error) {
	var sortCols SortColumns
	if sortColList == nil {
		sortCols, _ = newSortColumns(NewSortColumn("TABLE", utils.DESC))
		return &DistinctMerger{
			sortCols:        sortCols,
			distinctColumns: distinctCols,
		}, nil
	}
	if len(distinctCols) == 0 {
		return nil, errs.ErrDistinctColsIsNull
	}
	sortCols = *sortColList
	// 检查sortCols必须全在distinctCols
	distinctMap := make(map[string]struct{})
	for _, col := range distinctCols {
		_, ok := distinctMap[col.Name]
		if ok {
			return nil, errs.ErrDistinctColsRepeated
		} else {
			distinctMap[col.Name] = struct{}{}
		}
	}
	for i := 0; i < sortCols.Len(); i++ {
		val := sortCols.Get(i)
		if _, ok := distinctMap[val.name]; !ok {
			return nil, errs.ErrSortColListNotContainDistinctCol
		}
	}
	return &DistinctMerger{
		sortCols:        sortCols,
		distinctColumns: distinctCols,
		hasOrderBy:      true,
	}, nil
}
