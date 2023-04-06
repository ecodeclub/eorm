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

type Count struct {
	colInfos []ColInfo
	alias    string
}

func (s *Count) Aggregate(cols [][]any) (any, error) {
	var kind reflect.Kind
	if len(cols) >= 1 && len(cols[0]) >= 1 {
		kind = reflect.TypeOf(cols[0][0]).Kind()
	} else {
		return nil, errs.ErrMergerAggregateParticipant
	}
	return CountAggregateFuncMapping[kind](cols)
}

func (s *Count) ColumnInfo() []ColInfo {
	return s.colInfos
}
func (s *Count) ColumnName() string {
	return s.alias
}

func NewCount(info ColInfo, alias string) *Count {
	colInfos := []ColInfo{
		info,
	}
	return &Count{
		colInfos: colInfos,
		alias:    alias,
	}
}

func CountAggregate[T AggregateElement](cols [][]any) (any, error) {
	var Count T
	for _, col := range cols {
		Count += col[0].(T)
	}
	return Count, nil
}

var CountAggregateFuncMapping = map[reflect.Kind]func([][]any) (any, error){
	reflect.Int:     CountAggregate[int],
	reflect.Int8:    CountAggregate[int8],
	reflect.Int16:   CountAggregate[int16],
	reflect.Int32:   CountAggregate[int32],
	reflect.Int64:   CountAggregate[int64],
	reflect.Uint8:   CountAggregate[uint8],
	reflect.Uint16:  CountAggregate[uint16],
	reflect.Uint32:  CountAggregate[uint32],
	reflect.Uint64:  CountAggregate[uint64],
	reflect.Float32: CountAggregate[float32],
	reflect.Float64: CountAggregate[float64],
	reflect.Uint:    CountAggregate[uint],
}
