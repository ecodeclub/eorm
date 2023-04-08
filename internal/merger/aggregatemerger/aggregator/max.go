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

type Max struct {
	maxColumnInfo ColumnInfo
	alias         string
}

func (m *Max) Aggregate(cols [][]any) (any, error) {
	var kind reflect.Kind
	maxIndex := m.maxColumnInfo.Index
	if maxIndex < 0 || maxIndex >= len(cols[0]) {
		return nil, errs.ErrMergerInvalidAggregateColumnIndex
	}
	kind = reflect.TypeOf(cols[0][maxIndex]).Kind()

	maxFunc, ok := maxFuncMapping[kind]
	if !ok {
		return nil, errs.ErrMergerAggregateFuncNotFound
	}
	return maxFunc(cols, m.maxColumnInfo.Index)
}

func (m *Max) ColumnName() string {
	return m.alias
}

func NewMax(info ColumnInfo, alias string) *Max {
	return &Max{
		maxColumnInfo: info,
		alias:         alias,
	}
}

func maxAggregator[T AggregateElement](colsData [][]any, maxIndex int) (any, error) {
	return findExtremeValue[T](colsData, maxValue[T], maxIndex)
}

var maxFuncMapping = map[reflect.Kind]func([][]any, int) (any, error){
	reflect.Int:     maxAggregator[int],
	reflect.Int8:    maxAggregator[int8],
	reflect.Int16:   maxAggregator[int16],
	reflect.Int32:   maxAggregator[int32],
	reflect.Int64:   maxAggregator[int64],
	reflect.Uint8:   maxAggregator[uint8],
	reflect.Uint16:  maxAggregator[uint16],
	reflect.Uint32:  maxAggregator[uint32],
	reflect.Uint64:  maxAggregator[uint64],
	reflect.Float32: maxAggregator[float32],
	reflect.Float64: maxAggregator[float64],
	reflect.Uint:    maxAggregator[uint],
}

type ExtremeValueFunc[T AggregateElement] func(T, T) bool

func findExtremeValue[T AggregateElement](colsData [][]any, extremeValueFunc ExtremeValueFunc[T], index int) (any, error) {
	var ans T
	for idx, colData := range colsData {
		data := colData[index].(T)
		if idx == 0 {
			ans = data
			continue
		}
		if extremeValueFunc(ans, data) {
			ans = data
		}
	}
	return ans, nil
}

func maxValue[T AggregateElement](maxData T, data T) bool {
	return maxData < data
}
