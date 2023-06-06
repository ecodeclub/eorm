package utils

import (
	"database/sql/driver"
	"reflect"
	"time"
)

type Ordered interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64 | ~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 | ~float32 | ~float64 | ~string
}

type Order bool

const (
	// ASC 升序排序
	ASC Order = true
	// DESC 降序排序
	DESC Order = false
)

func Compare[T Ordered](ii any, jj any, order Order) int {
	i, j := ii.(T), jj.(T)
	if i < j && order == ASC || i > j && order == DESC {
		return -1
	} else if i > j && order == ASC || i < j && order == DESC {
		return 1
	} else {
		return 0
	}
}
func CompareBool(ii, jj any, order Order) int {
	i, j := ii.(bool), jj.(bool)
	if i == j {
		return 0
	}
	if i && !j {
		return 1
	}
	return -1
}

func CompareNullable(ii, jj any, order Order) int {
	i := ii.(driver.Valuer)
	j := jj.(driver.Valuer)
	iVal, _ := i.Value()
	jVal, _ := j.Value()
	// 如果i,j都为空返回0
	// 如果val返回为空永远是最小值
	if iVal == nil && jVal == nil {
		return 0
	} else if iVal == nil && order == ASC || jVal == nil && order == DESC {
		return -1
	} else if iVal == nil && order == DESC || jVal == nil && order == ASC {
		return 1
	}

	vali, ok := iVal.(time.Time)
	if ok {
		valj := jVal.(time.Time)
		return Compare[int64](vali.UnixMilli(), valj.UnixMilli(), order)
	}
	kind := reflect.TypeOf(iVal).Kind()
	return CompareFuncMapping[kind](iVal, jVal, order)
}

var CompareFuncMapping = map[reflect.Kind]func(any, any, Order) int{
	reflect.Int:     Compare[int],
	reflect.Int8:    Compare[int8],
	reflect.Int16:   Compare[int16],
	reflect.Int32:   Compare[int32],
	reflect.Int64:   Compare[int64],
	reflect.Uint8:   Compare[uint8],
	reflect.Uint16:  Compare[uint16],
	reflect.Uint32:  Compare[uint32],
	reflect.Uint64:  Compare[uint64],
	reflect.Float32: Compare[float32],
	reflect.Float64: Compare[float64],
	reflect.String:  Compare[string],
	reflect.Uint:    Compare[uint],
	reflect.Bool:    CompareBool,
}
