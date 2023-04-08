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

package aggregator

import (
	"reflect"

	"github.com/ecodeclub/eorm/internal/merger/internal/errs"
)

type Min struct {
	minColumnInfo ColumnInfo
	alias         string
}

func (m *Min) Aggregate(cols [][]any) (any, error) {
	var kind reflect.Kind
	minIndex := m.minColumnInfo.Index
	if minIndex < 0 || minIndex >= len(cols[0]) {
		return nil, errs.ErrMergerInvalidAggregateColumnIndex
	}
	kind = reflect.TypeOf(cols[0][minIndex]).Kind()
	minFunc, ok := minFuncMapping[kind]
	if !ok {
		return nil, errs.ErrMergerAggregateFuncNotFound
	}
	return minFunc(cols, m.minColumnInfo.Index)

}

func (m *Min) ColumnName() string {
	return m.alias
}

func NewMin(info ColumnInfo, alias string) *Min {
	return &Min{
		minColumnInfo: info,
		alias:         alias,
	}
}

func minAggregator[T AggregateElement](colsData [][]any, minIndex int) (any, error) {
	return findExtremeValue[T](colsData, minValue[T], minIndex)
}

var minFuncMapping = map[reflect.Kind]func([][]any, int) (any, error){
	reflect.Int:     minAggregator[int],
	reflect.Int8:    minAggregator[int8],
	reflect.Int16:   minAggregator[int16],
	reflect.Int32:   minAggregator[int32],
	reflect.Int64:   minAggregator[int64],
	reflect.Uint8:   minAggregator[uint8],
	reflect.Uint16:  minAggregator[uint16],
	reflect.Uint32:  minAggregator[uint32],
	reflect.Uint64:  minAggregator[uint64],
	reflect.Float32: minAggregator[float32],
	reflect.Float64: minAggregator[float64],
	reflect.Uint:    minAggregator[uint],
}

func minValue[T AggregateElement](minData T, data T) bool {
	return minData > data
}
