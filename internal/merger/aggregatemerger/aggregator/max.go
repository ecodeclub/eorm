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
	colInfos []ColInfo
	alias    string
}

func (m *Max) Aggregate(cols [][]any) (any, error) {
	var kind reflect.Kind
	if len(cols) >= 1 && len(cols[0]) >= 1 {
		kind = reflect.TypeOf(cols[0][0]).Kind()
	} else {
		return nil, errs.ErrMergerAggregateParticipant
	}
	return maxFuncMapping[kind](cols)

}

func (m *Max) ColumnInfo() []ColInfo {
	return m.colInfos
}

func (m *Max) ColumnName() string {
	return m.alias
}

func NewMax(info ColInfo, alias string) *Max {
	colInfos := []ColInfo{
		info,
	}
	return &Max{
		colInfos: colInfos,
		alias:    alias,
	}
}

func maxAggregator[T AggregateElement](colsData [][]any) (any, error) {
	var maxData T
	for idx, colData := range colsData {
		data := colData[0].(T)
		if idx == 0 {
			maxData = data
			continue
		}
		if maxData < data {
			maxData = data
		}
	}
	return maxData, nil
}

var maxFuncMapping = map[reflect.Kind]func([][]any) (any, error){
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
