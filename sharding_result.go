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

package eorm

import "database/sql"

type MultiExecRes struct {
	err error
	res []sql.Result
}

func (m MultiExecRes) Err() error {
	return m.err
}

func (m MultiExecRes) LastInsertId() (int64, error) {
	return m.res[len(m.res)-1].LastInsertId()
}
func (m MultiExecRes) RowsAffected() (int64, error) {
	var sum int64
	for _, r := range m.res {
		n, err := r.RowsAffected()
		if err != nil {
			return 0, err
		}
		sum += n
	}
	return sum, nil
}

func NewMultiExecRes(res []sql.Result) MultiExecRes {
	return MultiExecRes{res: res}
}
