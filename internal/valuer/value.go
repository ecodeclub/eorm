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

package valuer

import (
	"database/sql"
	"github.com/gotomicro/eorm/internal/model"
)

// Value 是对结构体实例的内部抽象
type Value interface {
	// Field 访问结构体字段, name 是字段名
	Field(name string) (any, error)
	// SetColumns 设置新值，column 是列名
	// 要注意，val 可能存在被上层复用，从而引起篡改的问题
	SetColumns(rows *sql.Rows) error
}

type Creator func(val any, meta *model.TableMeta) Value
