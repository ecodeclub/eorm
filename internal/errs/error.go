// Copyright 2021 gotomicro
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

package errs

import (
	"errors"
	"fmt"
)

var (
	ErrPointerOnly = errors.New("eorm: 只支持指向结构体的一级指针")
	errValueNotSet = errors.New("eorm: 值未设置")
	ErrNoRows      = errors.New("eorm: 未找到数据")
	// ErrTooManyColumns 过多列
	// 一般是查询的列多于结构体的列
	ErrTooManyColumns = errors.New("eorm: 过多列")

	// ErrCombinationIsNotStruct 不支持的组合类型，eorm 只支持结构体组合
	ErrCombinationIsNotStruct = errors.New("eorm: 不支持的组合类型，eorm 只支持结构体组合")
)

func NewFieldConflictError(field string) error {
	return fmt.Errorf("eorm: `%s`列冲突", field)
}

// NewInvalidFieldError 返回代表未知字段的错误。
// 通常来说，是字段名没写对
// 注意区分 NewInvalidColumnError
func NewInvalidFieldError(field string) error {
	return fmt.Errorf("eorm: 未知字段 %s", field)
}

// NewInvalidColumnError 返回代表未知列名的错误
// 通常来说，是列名不对
// 注意区分 NewInvalidFieldError
func NewInvalidColumnError(column string) error {
	return fmt.Errorf("eorm: 未知列 %s", column)
}

func NewValueNotSetError() error {
	return errValueNotSet
}

// NewUnsupportedDriverError 不支持驱动类型
func NewUnsupportedDriverError(driver string) error {
	return fmt.Errorf("eorm: 不支持driver类型 %s", driver)
}

// NewUnsupportedTableReferenceError 不支持的TableReference类型
func NewUnsupportedTableReferenceError(table any) error {
	return fmt.Errorf("eorm: 不支持的TableReference类型 %v", table)
}

func NewMustSpecifyColumnsError() error {
	return fmt.Errorf("eorm: 复合查询如 JOIN 查询、子查询必须指定要查找的列，即指定 SELECT xxx 部分")
}
