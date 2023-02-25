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

package dialect

import "github.com/ecodeclub/eorm/internal/errs"

// Dialect specify config or behavior of special SQL dialects
type Dialect struct {
	Name string
	// in MYSQL, it's "`"
	Quote byte
}

var (
	MySQL = Dialect{
		Name:  "MySQL",
		Quote: '`',
	}
	SQLite = Dialect{
		Name:  "SQLite",
		Quote: '`',
	}
)

func Of(driver string) (Dialect, error) {
	switch driver {
	case "sqlite3":
		return SQLite, nil
	case "mysql":
		return MySQL, nil
	default:
		return Dialect{}, errs.NewUnsupportedDriverError(driver)
	}
}
