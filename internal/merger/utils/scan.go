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

package utils

import (
	"database/sql"
	"reflect"
)

func Scan(row *sql.Rows) ([]any, error) {
	colsInfo, err := row.ColumnTypes()
	if err != nil {
		return nil, err
	}
	colsData := make([]any, 0, len(colsInfo))
	// 拿到sql.Rows字段的类型然后初始化
	for _, colInfo := range colsInfo {
		typ := colInfo.ScanType()
		// sqlite3的驱动返回的是指针。循环的去除指针
		for typ.Kind() == reflect.Pointer {
			typ = typ.Elem()
		}
		newData := reflect.New(typ).Interface()
		colsData = append(colsData, newData)
	}
	// 通过Scan赋值
	err = row.Scan(colsData...)
	if err != nil {
		return nil, err
	}
	// 去掉reflect.New的指针
	for i := 0; i < len(colsData); i++ {
		colsData[i] = reflect.ValueOf(colsData[i]).Elem().Interface()
	}
	return colsData, nil
}
