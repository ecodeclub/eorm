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

type AggregateElement interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64 | ~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 | ~float32 | ~float64
}

type Aggregator interface {
	// Aggregate 将多个列聚合
	Aggregate([][]any) (any, error)
	// ColumnInfo 返回需要进行聚合的列信息
	ColumnInfo() []ColInfo
	// ColumnName 返回
	ColumnName() string
}

type ColInfo struct {
	Index int
	Name  string
}

func NewColInfo(index int, name string) ColInfo {
	return ColInfo{
		Index: index,
		Name:  name,
	}
}
