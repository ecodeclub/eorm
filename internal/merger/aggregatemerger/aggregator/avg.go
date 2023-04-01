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

	"github.com/shopspring/decimal"
)

type AVG[S AggregateElement, C AggregateElement] struct {
	colMap  map[string]ColInfo
	colName string
}

func (a *AVG[S, C]) Aggregate(columns [][]any) (any, error) {
	var sum S
	var count C
	for _, col := range columns {
		colSum, _ := col[a.colMap["SUM"].Index].(S)
		sum += colSum
		colCount, _ := col[a.colMap["COUNT"].Index].(C)
		count += colCount
	}
	ans := decimal.NewFromFloat(float64(sum)).Div(decimal.NewFromFloat(float64(count)))
	ansf, _ := ans.Float64()
	return ansf, nil
}

func (a *AVG[S, C]) ColumnInfo() map[string]ColInfo {
	return a.colMap
}

func (a *AVG[S, C]) ColumnName() string {
	return a.colName
}

func NewAVG[S AggregateElement, C AggregateElement](sumName string, countName string, alias string) *AVG[S, C] {
	var sum S
	var count C
	sumTyp := reflect.TypeOf(sum)
	countType := reflect.TypeOf(count)
	colMap := make(map[string]ColInfo, 2)
	colMap["SUM"] = ColInfo{
		Index: 0,
		Name:  sumName,
		Typ:   sumTyp,
	}
	colMap["COUNT"] = ColInfo{
		Index: 1,
		Name:  countName,
		Typ:   countType,
	}
	return &AVG[S, C]{
		colMap:  colMap,
		colName: alias,
	}
}
