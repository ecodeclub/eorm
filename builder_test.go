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

package eorm

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func (q Query) string() string {
	return fmt.Sprintf("SQL: %s\nArgs: %#v\n", q.SQL, q.Args)
}

func TestQuerier_Exec(t *testing.T) {
	orm := memoryOrm()
	q := NewQuerier(orm, `CREATE TABLE groups (
   group_id INTEGER PRIMARY KEY,
   name TEXT NOT NULL
)`)
	res, err := q.Exec(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	affected, err := res.RowsAffected()
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, int64(0), affected)
}

func ExampleNewQuerier() {
	orm := memoryOrm()
	q := NewQuerier(orm, `SELECT * FROM user_tab WHERE id = ?;`, 1)
	fmt.Printf(`
SQL: %s
Args: %v
`, q.q.SQL, q.q.Args)
	// Output:
	// SQL: SELECT * FROM user_tab WHERE id = ?;
	// Args: [1]
}

func ExampleQuerier_Exec() {
	orm := memoryOrm()
	q := NewQuerier(orm, `CREATE TABLE IF NOT EXISTS groups (
   group_id INTEGER PRIMARY KEY,
   name TEXT NOT NULL
)`)
	_, err := q.Exec(context.Background())
	if err == nil {
		fmt.Print("SUCCESS")
	}
	// Output:
	// SUCCESS
}