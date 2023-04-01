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

type Min[T AggregateElement] struct {
	colMap  map[string]ColInfo
	colName string
}

func (m *Min[T]) Aggregate(columns [][]any) (any, error) {
	ans := make([]T, 0, 1)
	for _, col := range columns {
		data, _ := col[0].(T)
		if len(ans) == 0 {
			ans = append(ans, data)
		} else if ans[0] > data {
			ans[0] = data
		}
	}
	return ans[0], nil
}

func (m *Min[T]) ColumnInfo() map[string]ColInfo {
	return m.colMap
}

func (m *Min[T]) ColumnName() string {
	return m.colName
}

// NewMin 第一个参数为数据库里的列名，第二个为返回的列名
func NewMin[T AggregateElement](colName string, name string) *Min[T] {
	var t T
	typ := reflect.TypeOf(t)
	colMap := make(map[string]ColInfo, 1)
	colMap["MIN"] = ColInfo{
		Index: 0,
		Name:  colName,
		Typ:   typ,
	}

	return &Min[T]{
		colMap:  colMap,
		colName: name,
	}
}
