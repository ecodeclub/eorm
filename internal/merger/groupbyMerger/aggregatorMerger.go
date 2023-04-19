package groupbyMerger

import (
	"context"
	"database/sql"
	"reflect"
	"sync"
	_ "unsafe"

	"github.com/ecodeclub/eorm/internal/merger"
	"github.com/ecodeclub/eorm/internal/merger/aggregatemerger/aggregator"
	"github.com/ecodeclub/eorm/internal/merger/internal/errs"
	"github.com/gotomicro/ekit/mapx"
)

//go:linkname convertAssign database/sql.convertAssign
func convertAssign(dest, src any) error

type GroupByColumn struct {
	Index int
	Name  string
}

func NewGroupByColumn(index int, name string) GroupByColumn {
	return GroupByColumn{
		Index: index,
		Name:  name,
	}
}

type AggregatorMerger struct {
	aggregators  []aggregator.Aggregator
	groupColumns []GroupByColumn
	columnsName  []string
}

func NewAggregatorMerger(aggregators []aggregator.Aggregator, groupColumns []GroupByColumn) *AggregatorMerger {
	cols := make([]string, 0, len(aggregators)+len(groupColumns))
	for _, groubyCol := range groupColumns {
		cols = append(cols, groubyCol.Name)
	}
	for _, agg := range aggregators {
		cols = append(cols, agg.ColumnName())
	}

	return &AggregatorMerger{
		aggregators:  aggregators,
		groupColumns: groupColumns,
		columnsName:  cols,
	}
}
func (a *AggregatorMerger) Merge(ctx context.Context, results []*sql.Rows) (merger.Rows, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if len(results) == 0 {
		return nil, errs.ErrMergerEmptyRows
	}

	for _, res := range results {
		err := a.checkColumns(res)
		if err != nil {
			return nil, err
		}
	}
	dataMap, dataIndex, err := a.getCols(results)
	if err != nil {
		return nil, err
	}

	return &AggregatorRows{
		rowsList:     results,
		aggregators:  a.aggregators,
		groupColumns: a.groupColumns,
		mu:           &sync.RWMutex{},
		dataMap:      dataMap,
		dataIndex:    dataIndex,
		cur:          -1,
		cols:         a.columnsName,
	}, nil

}
func (m *AggregatorMerger) checkColumns(rows *sql.Rows) error {
	if rows == nil {
		return errs.ErrMergerRowsIsNull
	}
	return nil
}

func (a *AggregatorMerger) getCols(rowsList []*sql.Rows) (*mapx.TreeMap[Key, [][]any], []Key, error) {
	treeMap, err := mapx.NewTreeMap[Key, [][]any](compareKey)
	if err != nil {
		return nil, nil, err
	}
	keys := make([]Key, 0)
	for _, res := range rowsList {
		colsData, err := a.getCol(res)
		if err != nil {
			return nil, nil, err
		}
		for _, colData := range colsData {
			key := Key{columnValues: make([]any, 0, len(a.groupColumns))}
			for _, groupByCol := range a.groupColumns {
				key.columnValues = append(key.columnValues, colData[groupByCol.Index])
			}
			val, ok := treeMap.Get(key)

			if ok {
				val = append(val, colData)
				err = treeMap.Set(key, val)
				if err != nil {
					return nil, nil, err
				}
			} else {
				keys = append(keys, key)
				err := treeMap.Put(key, [][]any{colData})
				if err != nil {
					return nil, nil, err
				}
			}
		}
	}
	return treeMap, keys, nil
}

func (a *AggregatorMerger) getCol(row *sql.Rows) ([][]any, error) {
	colsInfo, err := row.ColumnTypes()
	if err != nil {
		return nil, err
	}
	// colsData 表示一个sql.Rows的数据
	ans := make([][]any, 0)
	for row.Next() {
		colsData := make([]any, 0, len(colsInfo))
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
			return nil, err
		}
		// 去掉reflect.New的指针
		for i := 0; i < len(colsData); i++ {
			colsData[i] = reflect.ValueOf(colsData[i]).Elem().Interface()
		}
		ans = append(ans, colsData)
	}
	if row.Err() != nil {
		return nil, row.Err()
	}

	return ans, nil

}

type AggregatorRows struct {
	rowsList     []*sql.Rows
	aggregators  []aggregator.Aggregator
	groupColumns []GroupByColumn
	dataMap      *mapx.TreeMap[Key, [][]any]
	cur          int
	dataIndex    []Key
	mu           *sync.RWMutex
	curData      []any
	closed       bool
	lastErr      error
	cols         []string
}

// Next 返回列的顺序先分组信息然后是聚合函数信息
func (a *AggregatorRows) Next() bool {
	a.mu.Lock()
	if a.closed {
		a.mu.Unlock()
		return false
	}
	a.cur++
	if a.cur >= len(a.dataIndex) {
		a.mu.Unlock()
		_ = a.Close()
		return false
	}
	a.curData = make([]any, 0, len(a.aggregators)+len(a.groupColumns))

	a.curData = append(a.curData, a.dataIndex[a.cur].columnValues...)

	for _, agg := range a.aggregators {
		val, _ := a.dataMap.Get(a.dataIndex[a.cur])
		res, err := agg.Aggregate(val)
		if err != nil {
			a.lastErr = err
			a.mu.Unlock()
			_ = a.Close()
			return false
		}
		a.curData = append(a.curData, res)
	}

	a.mu.Unlock()
	return true
}

func (a *AggregatorRows) Scan(dest ...any) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.lastErr != nil {
		return a.lastErr
	}
	if a.closed {
		return errs.ErrMergerRowsClosed
	}
	if a.cur == -1 {
		return errs.ErrMergerScanNotNext
	}
	for i := 0; i < len(dest); i++ {
		err := convertAssign(dest[i], a.curData[i])
		if err != nil {
			return err
		}
	}
	return nil
}

// Close 关闭所有的sql.Rows
func (a *AggregatorRows) Close() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.closed = true
	return nil
}

// Columns 返回列的顺序先分组信息然后是聚合函数信息
func (a *AggregatorRows) Columns() ([]string, error) {
	a.mu.RLock()
	defer a.mu.RUnlock()
	if a.closed {
		return nil, errs.ErrMergerRowsClosed
	}
	return a.cols, nil
}

func (a *AggregatorRows) Err() error {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.lastErr
}

type Key struct {
	columnValues []any
}

func compareKey(a, b Key) int {
	keyLen := len(a.columnValues)
	for i := 0; i < keyLen; i++ {
		compareFunc := compareFuncMapping[reflect.TypeOf(a.columnValues[i]).Kind()]
		res := compareFunc(a.columnValues[i], b.columnValues[i])
		if res != 0 {
			return res
		}
	}
	return 0
}

func compare[T Ordered](ii any, jj any) int {
	i, j := ii.(T), jj.(T)
	if i < j {
		return -1
	} else if i > j {
		return 1
	} else {
		return 0
	}
}

type Ordered interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64 | ~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 | ~float32 | ~float64 | ~string
}

var compareFuncMapping = map[reflect.Kind]func(any, any) int{
	reflect.Int:     compare[int],
	reflect.Int8:    compare[int8],
	reflect.Int16:   compare[int16],
	reflect.Int32:   compare[int32],
	reflect.Int64:   compare[int64],
	reflect.Uint8:   compare[uint8],
	reflect.Uint16:  compare[uint16],
	reflect.Uint32:  compare[uint32],
	reflect.Uint64:  compare[uint64],
	reflect.Float32: compare[float32],
	reflect.Float64: compare[float64],
	reflect.String:  compare[string],
	reflect.Uint:    compare[uint],
	reflect.Bool:    compareBool,
}

func compareBool(ii, jj any) int {
	i, j := ii.(bool), jj.(bool)
	if i == j {
		return 0
	}
	if i && !j {
		return 1
	}
	return -1
}
