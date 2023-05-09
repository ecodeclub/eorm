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
	"database/sql/driver"
	_ "unsafe"
)

//go:linkname sqlConvertAssign database/sql.convertAssign
func sqlConvertAssign(dest, src any) error

func ConvertAssign(dest, src any) error {
	srcVal, ok := src.(driver.Valuer)
	if ok {
		var err error
		src, err = srcVal.Value()
		if err != nil {
			return err
		}
	}
	return sqlConvertAssign(dest, src)
}
