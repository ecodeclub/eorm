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

import (
	"context"
	"database/sql"

	"github.com/ecodeclub/eorm/internal/datasource"
	"github.com/ecodeclub/eorm/internal/datasource/single"
	"github.com/ecodeclub/eorm/internal/dialect"
	"github.com/ecodeclub/eorm/internal/errs"
	"github.com/ecodeclub/eorm/internal/model"
	"github.com/ecodeclub/eorm/internal/valuer"
)

const (
	SELECT = "SELECT"
	DELETE = "DELETE"
	UPDATE = "UPDATE"
	INSERT = "INSERT"
	RAW    = "RAW"
)

// DBOption configure DB
type DBOption func(db *DB)

// DB represents a database
type DB struct {
	baseSession
	ds datasource.DataSource
}

// DBWithMiddlewares 为 db 配置 Middleware
func DBWithMiddlewares(ms ...Middleware) DBOption {
	return func(db *DB) {
		db.ms = ms
	}
}

func DBWithMetaRegistry(r model.MetaRegistry) DBOption {
	return func(db *DB) {
		db.metaRegistry = r
	}
}

func UseReflection() DBOption {
	return func(db *DB) {
		db.valCreator = valuer.PrimitiveCreator{Creator: valuer.NewUnsafeValue}
	}
}

// Open 创建一个 ORM 实例
// 注意该实例是一个无状态的对象，你应该尽可能复用它
func Open(driver string, dsn string, opts ...DBOption) (*DB, error) {
	db, err := single.OpenDB(driver, dsn)
	if err != nil {
		return nil, err
	}
	return OpenDS(driver, db, opts...)
}

func OpenDS(driver string, ds datasource.DataSource, opts ...DBOption) (*DB, error) {
	dl, err := dialect.Of(driver)
	if err != nil {
		return nil, err
	}
	orm := &DB{
		baseSession: baseSession{
			executor: ds,
			core: core{
				metaRegistry: model.NewMetaRegistry(),
				dialect:      dl,
				// 可以设为默认，因为原本这里也有默认
				valCreator: valuer.PrimitiveCreator{
					Creator: valuer.NewUnsafeValue,
				},
			},
		},
		ds: ds,
	}
	for _, o := range opts {
		o(orm)
	}
	return orm, nil
}

func (db *DB) BeginTx(ctx context.Context, opts *sql.TxOptions) (*Tx, error) {
	inst, ok := db.ds.(datasource.TxBeginner)
	if !ok {
		return nil, errs.ErrNotCompleteTxBeginner
	}
	tx, err := inst.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	return &Tx{tx: tx, baseSession: baseSession{
		executor: tx,
		core:     db.core,
	}}, nil
}

func (db *DB) Close() error {
	return db.ds.Close()
}
