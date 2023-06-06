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

package sortmerger

import (
	"database/sql/driver"
	"reflect"

	"github.com/ecodeclub/eorm/internal/merger/utils"
)

type Heap struct {
	h           []*node
	sortColumns SortColumns
}

func (h *Heap) Len() int {
	return len(h.h)
}

func (h *Heap) Less(i, j int) bool {
	for k := 0; k < h.sortColumns.Len(); k++ {
		valueI := h.h[i].sortCols[k]
		valueJ := h.h[j].sortCols[k]
		_, ok := valueJ.(driver.Valuer)
		var cp func(any, any, utils.Order) int
		if ok {
			cp = utils.CompareNullable
		} else {
			kind := reflect.TypeOf(valueI).Kind()
			cp = utils.CompareFuncMapping[kind]
		}
		res := cp(valueI, valueJ, h.sortColumns.Get(k).order)
		if res == 0 {
			continue
		}
		if res == -1 {
			return true
		}
		return false
	}
	return false
}

func (h *Heap) Swap(i, j int) {
	h.h[i], h.h[j] = h.h[j], h.h[i]
}

func (h *Heap) Push(x any) {
	h.h = append(h.h, x.(*node))
}

func (h *Heap) Pop() any {
	v := h.h[len(h.h)-1]
	h.h = h.h[:len(h.h)-1]
	return v
}

type node struct {
	index    int
	sortCols []any
	columns  []any
}
