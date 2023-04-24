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
	"database/sql"
	"reflect"
)

var compareFuncMapping = map[reflect.Kind]func(any, any, Order) int{
	reflect.Int:     compare[int],
	reflect.Int8:    compare[int8],
	reflect.Int16:   compare[int16],
	reflect.Int32:   compare[int32],
	reflect.Int64:   compare[int64],
	reflect.Uint8:   compare[uint8],
	reflect.Uint16:  compare[uint16],
	reflect.Uint32:  compare[uint32],
	reflect.Uint64:  compare[uint64],
	reflect.Float32: compare[float32],
	reflect.Float64: compare[float64],
	reflect.String:  compare[string],
	reflect.Uint:    compare[uint],
	reflect.Struct:  compareNullable,
}

type Heap struct {
	h           []*node
	sortColumns sortColumns
}

func (h *Heap) Len() int {
	return len(h.h)
}

func (h *Heap) Less(i, j int) bool {
	for k := 0; k < h.sortColumns.Len(); k++ {
		valueI := h.h[i].sortCols[k]
		valueJ := h.h[j].sortCols[k]
		kind := reflect.TypeOf(valueI).Kind()
		cp := compareFuncMapping[kind]
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

// 升序时， -1 表示 i < j, 1 表示i > j ,0 表示两者相同
// 降序时，-1 表示 i > j, 1 表示 i < j ,0 表示两者相同

func compare[T Ordered](ii any, jj any, order Order) int {
	i, j := ii.(T), jj.(T)
	if i < j && order || i > j && !order {
		return -1
	} else if i > j && order || i < j && !order {
		return 1
	} else {
		return 0
	}
}

func compareNullable(ii any, jj any, order Order) int {
	switch ii.(type) {
	case sql.NullString:
		i, j := ii.(sql.NullString), jj.(sql.NullString)
		if !i.Valid && !j.Valid {
			return 0
		}
		if !i.Valid && order == ASC || !j.Valid && order == DESC {
			return -1
		} else if !i.Valid && order == DESC || !j.Valid && order == ASC {
			return 1
		}
		return compare[string](i.String, j.String, order)
	case sql.NullFloat64:
		i, j := ii.(sql.NullFloat64), jj.(sql.NullFloat64)
		if !i.Valid && !j.Valid {
			return 0
		}
		if !i.Valid && order == ASC || !j.Valid && order == DESC {
			return -1
		} else if !i.Valid && order == DESC || !j.Valid && order == ASC {
			return 1
		}
		return compare[float64](i.Float64, j.Float64, order)
	case sql.NullInt64:
		i, j := ii.(sql.NullInt64), jj.(sql.NullInt64)
		if !i.Valid && !j.Valid {
			return 0
		}
		if !i.Valid && order == ASC || !j.Valid && order == DESC {
			return -1
		} else if !i.Valid && order == DESC || !j.Valid && order == ASC {
			return 1
		}
		return compare[int64](i.Int64, j.Int64, order)
	case sql.NullInt16:
		i, j := ii.(sql.NullInt16), jj.(sql.NullInt16)
		if !i.Valid && !j.Valid {
			return 0
		}
		if !i.Valid && order == ASC || !j.Valid && order == DESC {
			return -1
		} else if !i.Valid && order == DESC || !j.Valid && order == ASC {
			return 1
		}
		return compare[int16](i.Int16, j.Int16, order)
	case sql.NullInt32:
		i, j := ii.(sql.NullInt32), jj.(sql.NullInt32)
		if !i.Valid && !j.Valid {
			return 0
		}
		if !i.Valid && order == ASC || !j.Valid && order == DESC {
			return -1
		} else if !i.Valid && order == DESC || !j.Valid && order == ASC {
			return 1
		}
		return compare[int32](i.Int32, j.Int32, order)
	case sql.NullByte:
		i, j := ii.(sql.NullByte), jj.(sql.NullByte)
		if !i.Valid && !j.Valid {
			return 0
		}
		if !i.Valid && order == ASC || !j.Valid && order == DESC {
			return -1
		} else if !i.Valid && order == DESC || !j.Valid && order == ASC {
			return 1
		}
		return compare[byte](i.Byte, j.Byte, order)
	case sql.NullTime:
		i, j := ii.(sql.NullTime), jj.(sql.NullTime)
		if !i.Valid && !j.Valid {
			return 0
		}
		if !i.Valid && order == ASC || !j.Valid && order == DESC {
			return -1
		} else if !i.Valid && order == DESC || !j.Valid && order == ASC {
			return 1
		}
		vali := i.Time.UnixMilli()
		valj := j.Time.UnixMilli()
		return compare[int64](vali, valj, order)
	default:
		return 0
	}
}
