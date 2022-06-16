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
	"github.com/gotomicro/eorm/internal/dialect"
	"github.com/gotomicro/eorm/internal/model"
	"github.com/gotomicro/eorm/internal/valuer"
	"github.com/valyala/bytebufferpool"
	"log"
	"time"
)

// DBOption configure DB
type DBOption func(db *DB)

// DB represents a database
type DB struct {
	db *sql.DB
	core
}

func (db *DB) queryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return db.db.QueryContext(ctx, query, args...)
}

func (db *DB) execContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return db.db.ExecContext(ctx, query, args...)
}

// Open 创建一个 ORM 实例
// 注意该实例是一个无状态的对象，你应该尽可能复用它
func Open(driver string, dsn string, opts...DBOption) (*DB, error) {
	db, err := sql.Open(driver, dsn)
	if err != nil {
		return nil, err
	}
	return openDB(driver, db, opts...)
}

func openDB(driver string, db *sql.DB, opts...DBOption) (*DB, error) {
	dl, err := dialect.Of(driver)
	if err != nil {
		return nil, err
	}
	orm := &DB{
		core: core {
			metaRegistry: model.NewMetaRegistry(),
			dialect:      dl,
			valCreator: valuer.NewUnsafeValue,
		},
		db: db,
	}
	for _, o := range opts {
		o(orm)
	}
	return orm, nil
}

// WithMetaRegistry 指定元数据注册中心
func WithMetaRegistry(registry model.MetaRegistry) DBOption {
	return func(db *DB) {
		db.metaRegistry = registry
	}
}

// WithDialect 指定方言
func WithDialect(dialect dialect.Dialect) DBOption {
	return func(db *DB) {
		db.dialect = dialect
	}
}

func (db *DB) BeginTx(ctx context.Context, opts *sql.TxOptions) (*Tx, error) {
	tx, err := db.db.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	return &Tx{tx: tx, db: db}, nil
}

// Delete 开始构建一个 DELETE 查询
func (db *DB) Delete() *Deleter {
	return &Deleter{
		builder: builder{
			core: db.core,
			buffer: bytebufferpool.Get(),
		},
	}
}

// Update 开始构建一个 UPDATE 查询
func (db *DB) Update(table interface{}) *Updater {
	return &Updater{
		builder: builder{
			core: db.core,
			buffer: bytebufferpool.Get(),
		},
		table:   table,
	}
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