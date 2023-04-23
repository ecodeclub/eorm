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

	"github.com/ecodeclub/eorm/internal/merger"

	"github.com/ecodeclub/eorm/internal/merger/internal/errs"
)

type Sum struct {
	sumColumnInfo merger.ColumnInfo
}

func (s *Sum) Aggregate(cols [][]any) (any, error) {
	sumFunc, err := s.findSumFunc(cols[0])
	if err != nil {
		return nil, err
	}
	return sumFunc(cols, s.sumColumnInfo.Index)
}

func (s *Sum) findSumFunc(col []any) (func([][]any, int) (any, error), error) {
	var kind reflect.Kind
	sumIndex := s.sumColumnInfo.Index
	if sumIndex < 0 || sumIndex >= len(col) {
		return nil, errs.ErrMergerInvalidAggregateColumnIndex
	}
	kind = reflect.TypeOf(col[sumIndex]).Kind()
	sumFunc, ok := sumAggregateFuncMapping[kind]
	if !ok {
		return nil, errs.ErrMergerAggregateFuncNotFound
	}
	return sumFunc, nil
}

func (s *Sum) ColumnName() string {
	return s.sumColumnInfo.Name
}

func NewSum(info merger.ColumnInfo) *Sum {
	return &Sum{
		sumColumnInfo: info,
	}
}

func sumAggregate[T AggregateElement](cols [][]any, sumIndex int) (any, error) {
	var sum T
	for _, col := range cols {
		sum += col[sumIndex].(T)
	}
	return sum, nil
}

var sumAggregateFuncMapping = map[reflect.Kind]func([][]any, int) (any, error){
	reflect.Int:     sumAggregate[int],
	reflect.Int8:    sumAggregate[int8],
	reflect.Int16:   sumAggregate[int16],
	reflect.Int32:   sumAggregate[int32],
	reflect.Int64:   sumAggregate[int64],
	reflect.Uint8:   sumAggregate[uint8],
	reflect.Uint16:  sumAggregate[uint16],
	reflect.Uint32:  sumAggregate[uint32],
	reflect.Uint64:  sumAggregate[uint64],
	reflect.Float32: sumAggregate[float32],
	reflect.Float64: sumAggregate[float64],
	reflect.Uint:    sumAggregate[uint],
}
