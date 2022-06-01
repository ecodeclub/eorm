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
	"reflect"
)

var (
	errValueNotSet = errors.New("eorm: 值未设置")
)


// NewInvalidColumnError returns an error represents invalid field name
func NewInvalidColumnError(field string) error {
	return fmt.Errorf("eorm: 未知字段 %s", field)
}

func NewValueNotSetError() error {
	return errValueNotSet
}

// NewUnsupportedTypeError 不支持的字段类型
// 请参阅 https://github.com/gotomicro/eorm/discussions/71
func NewUnsupportedTypeError(typ reflect.Type) error {
	return fmt.Errorf("eorm: 不支持字段类型 %s, %s", typ.PkgPath(), typ.Name())
}

func NewUnsupportedDriverError(driver string) error {
	return fmt.Errorf("eorm: 不支持driver类型 %s", driver)
}
