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

package batchmerger

import (
	"context"
	"database/sql"
	"sync"

	"github.com/ecodeclub/eorm/internal/merger"
	"github.com/ecodeclub/eorm/internal/merger/internal/errs"
)

type Merger struct{}

func (Merger) Merge(ctx context.Context, results []*sql.Rows) (merger.Rows, error) {

	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if len(results) == 0 {
		return nil, errs.ErrMergerEmptyRows
	}
	for i := 0; i < len(results); i++ {
		if results[i] == nil {
			return nil, errs.ErrMergerRowsIsNull
		}
	}
	return &MergerRows{
		rows: results,
		mu:   &sync.RWMutex{},
	}, nil
}

type MergerRows struct {
	rows []*sql.Rows
	cnt  int
	mu   *sync.RWMutex
	once sync.Once
}

func (m *MergerRows) Next() bool {
	m.mu.RLock()
	if m.cnt >= len(m.rows) {
		m.mu.RUnlock()
		return false
	}
	if m.rows[m.cnt].Next() {
		m.mu.RUnlock()
		return true
	}
	m.mu.RUnlock()
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.cnt >= len(m.rows) {
		return false
	}
	if m.rows[m.cnt].Next() {
		return true
	}
	for {
		m.cnt++
		if m.cnt >= len(m.rows) {
			break
		}
		if m.rows[m.cnt].Next() {
			return true
		}

	}
	return false

}

func (m *MergerRows) Scan(dest ...any) error {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.rows[m.cnt].Scan(dest...)

}

func (m *MergerRows) Close() error {
	var err error
	m.once.Do(func() {
		for i := 0; i < len(m.rows); i++ {
			row := m.rows[i]
			err = row.Close()
			if err != nil {
				return
			}
		}
	})
	return err
}

func (m *MergerRows) Columns() ([]string, error) {
	return m.rows[0].Columns()
}

func (*MergerRows) Err() error {
	panic("implement me")
}
