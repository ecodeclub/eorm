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
	colInfos []ColInfo
	alias    string
}

func (m *Min) Aggregate(cols [][]any) (any, error) {
	var kind reflect.Kind
	if len(cols) >= 1 && len(cols[0]) >= 1 {
		kind = reflect.TypeOf(cols[0][0]).Kind()
	} else {
		return nil, errs.ErrMergerAggregateParticipant
	}
	return minFuncMapping[kind](cols)

}

func (m *Min) ColumnInfo() []ColInfo {
	return m.colInfos
}

func (m *Min) ColumnName() string {
	return m.alias
}

func NewMin(info ColInfo, alias string) *Min {
	colInfos := []ColInfo{
		info,
	}
	return &Min{
		colInfos: colInfos,
		alias:    alias,
	}
}

func minAggregator[T AggregateElement](colsData [][]any) (any, error) {
	var minData T
	for idx, colData := range colsData {
		data := colData[0].(T)
		if idx == 0 {
			minData = data
			continue
		}
		if minData > data {
			minData = data
		}
	}
	return minData, nil
}

var minFuncMapping = map[reflect.Kind]func([][]any) (any, error){
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
