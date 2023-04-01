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
)

type Max[T AggregateElement] struct {
	colMap  map[string]ColInfo
	colName string
}

func (m *Max[T]) Aggregate(columns [][]any) (any, error) {
	ans := make([]T, 0, 1)
	for _, col := range columns {
		data, _ := col[0].(T)
		if len(ans) == 0 {
			ans = append(ans, data)
		} else if ans[0] < data {
			ans[0] = data
		}
	}
	return ans[0], nil
}

func (m *Max[T]) ColumnInfo() map[string]ColInfo {
	return m.colMap
}

func (m *Max[T]) ColumnName() string {
	return m.colName
}

// NewMax 第一个参数为数据库里的列名，第二个为返回的列名
func NewMax[T AggregateElement](colName string, name string) *Max[T] {
	var t T
	typ := reflect.TypeOf(t)
	colMap := make(map[string]ColInfo, 1)
	colMap["MAX"] = ColInfo{
		Index: 0,
		Name:  colName,
		Typ:   typ,
	}
	return &Max[T]{
		colMap:  colMap,
		colName: name,
	}
}
