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
	_ "database/sql"
	"database/sql/driver"
	"reflect"
	_ "unsafe"

	"github.com/ecodeclub/eorm/internal/merger/internal/errs"
)

//go:linkname convertAssign database/sql.convertAssign
func convertAssign(dest, src any) error

func ConvertAssign(dest, src any) error {
	kind := reflect.TypeOf(src).Kind()
	if kind == reflect.Struct {
		return assertNullable(dest, src)
	}
	return convertAssign(dest, src)
}

func assertNullable(dest, src any) error {
	destScanner, ok := dest.(ScannerAndValuer)
	if !ok {
		return errs.ErrMergerNullAble
	}
	srcValuer, ok := src.(driver.Valuer)
	if !ok {
		return errs.ErrMergerNullAble
	}
	val, err := srcValuer.Value()
	if err != nil {
		return err
	}
	return destScanner.Scan(val)
}

type ScannerAndValuer interface {
	sql.Scanner
	driver.Valuer
}
