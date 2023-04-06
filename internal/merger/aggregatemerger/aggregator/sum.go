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

type Sum struct {
	colInfos []ColInfo
	alias    string
}

func (s *Sum) Aggregate(cols [][]any) (any, error) {
	var kind reflect.Kind
	if len(cols) >= 1 && len(cols[0]) >= 1 {
		kind = reflect.TypeOf(cols[0][0]).Kind()
	} else {
		return nil, errs.ErrMergerAggregateParticipant
	}
	return sumAggregateFuncMapping[kind](cols)
}

func (s *Sum) ColumnInfo() []ColInfo {
	return s.colInfos
}
func (s *Sum) ColumnName() string {
	return s.alias
}

func NewSUM(info ColInfo, alias string) *Sum {
	colInfos := []ColInfo{
		info,
	}
	return &Sum{
		colInfos: colInfos,
		alias:    alias,
	}
}

func sumAggregate[T AggregateElement](cols [][]any) (any, error) {
	var sum T
	for _, col := range cols {
		sum += col[0].(T)
	}
	return sum, nil
}

var sumAggregateFuncMapping = map[reflect.Kind]func([][]any) (any, error){
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
