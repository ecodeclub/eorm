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

import "reflect"

type Count[T AggregateElement] struct {
	colMap  map[string]ColInfo
	colName string
}

func (s *Count[T]) Aggregate(cols [][]any) (any, error) {
	var totalCount T
	for _, col := range cols {
		colValue, _ := col[0].(T)
		totalCount += colValue
	}
	return totalCount, nil
}

func (s *Count[T]) ColumnInfo() map[string]ColInfo {
	return s.colMap
}

func (s *Count[T]) ColumnName() string {
	return s.colName
}

// NewCount 第一个参数为数据库里的列名，第二个为返回的列名
func NewCount[T AggregateElement](colName string, alias string) *Count[T] {
	colMap := make(map[string]ColInfo, 1)
	var t T
	colMap["COUNT"] = ColInfo{
		Index: 0,
		Name:  colName,
		Typ:   reflect.TypeOf(t),
	}
	return &Count[T]{
		colMap:  colMap,
		colName: alias,
	}
}
