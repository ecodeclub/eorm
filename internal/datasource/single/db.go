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

package single

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"log"
	"time"

	"github.com/ecodeclub/eorm/internal/datasource"

	"github.com/ecodeclub/eorm/internal/datasource/transaction"
)

var _ datasource.TxBeginner = &DB{}
var _ datasource.DataSource = &DB{}

// DB represents a database
type DB struct {
	db *sql.DB
}

func (db *DB) Query(ctx context.Context, query datasource.Query) (*sql.Rows, error) {
	return db.db.QueryContext(ctx, query.SQL, query.Args...)
}

func (db *DB) Exec(ctx context.Context, query datasource.Query) (sql.Result, error) {
	return db.db.ExecContext(ctx, query.SQL, query.Args...)
}

func OpenDB(driver string, dsn string) (*DB, error) {
	db, err := sql.Open(driver, dsn)
	if err != nil {
		return nil, err
	}
	return &DB{db: db}, nil
}

func NewDB(db *sql.DB) *DB {
	return &DB{db: db}
}

func (db *DB) BeginTx(ctx context.Context, opts *sql.TxOptions) (datasource.Tx, error) {
	tx, err := db.db.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	return transaction.NewTx(tx, db), nil
}

// Wait 会等待数据库连接
// 注意只能用于测试
func (db *DB) Wait() error {
	err := db.db.Ping()
	for err == driver.ErrBadConn {
		log.Printf("等待数据库启动...")
		err = db.db.Ping()
		time.Sleep(time.Second)
	}
	return err
}

func (db *DB) Close() error {
	return db.db.Close()
}
