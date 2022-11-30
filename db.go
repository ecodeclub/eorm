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
	"database/sql"
	"database/sql/driver"
	"log"
	"time"

	"github.com/gotomicro/eorm/internal/dialect"
	"github.com/gotomicro/eorm/internal/model"
	"github.com/gotomicro/eorm/internal/valuer"
)

// DBOption configure DB
type DBOption func(db *DB)

// DB represents a database
type DB struct {
	db *sql.DB
	core
}

func DBWithValCreator(c valuer.Creator) DBOption {
	return func(db *DB) {
		db.valCreator = valuer.BasicTypeCreator{Creator: c}
	}
}

func (db *DB) queryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return db.db.QueryContext(ctx, query, args...)
}

func (db *DB) execContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return db.db.ExecContext(ctx, query, args...)
}

// Open 创建一个 ORM 实例
// 注意该实例是一个无状态的对象，你应该尽可能复用它
func Open(driver string, dsn string, opts ...DBOption) (*DB, error) {
	db, err := sql.Open(driver, dsn)
	if err != nil {
		return nil, err
	}
	return openDB(driver, db, opts...)
}

func openDB(driver string, db *sql.DB, opts ...DBOption) (*DB, error) {
	dl, err := dialect.Of(driver)
	if err != nil {
		return nil, err
	}
	orm := &DB{
		core: core{
			metaRegistry: model.NewMetaRegistry(),
			dialect:      dl,
			// 可以设为默认，因为原本这里也有默认
			valCreator: valuer.BasicTypeCreator{
				Creator: valuer.NewUnsafeValue,
			},
		},
		db: db,
	}
	for _, o := range opts {
		o(orm)
	}
	return orm, nil
}

// BeginTx 开启事务
func (db *DB) BeginTx(ctx context.Context, opts *sql.TxOptions) (*Tx, error) {
	tx, err := db.db.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	return &Tx{tx: tx, db: db}, nil
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

func (db *DB) getCore() core {
	return db.core
}
