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

package rows

import (
	"database/sql"
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
	// 预处理一下 sqlConvertAssign 不支持的转换，遇到一个加一个
	switch sv := src.(type) {
	case sql.RawBytes:
		switch dv := dest.(type) {
		case *string:
			*dv = string(sv)
			return nil
		}
	}
	return sqlConvertAssign(dest, src)
}
