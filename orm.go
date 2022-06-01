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
	"database/sql"
	"database/sql/driver"
	"github.com/gotomicro/eorm/internal/dialect"
	"github.com/gotomicro/eorm/internal/model"
	"github.com/gotomicro/eorm/internal/valuer"
	"github.com/valyala/bytebufferpool"
	"log"
	"time"
)

// OrmOption configure Orm
type OrmOption func(db *Orm)

// Orm represents a database
type Orm struct {
	db *sql.DB
	metaRegistry model.MetaRegistry
	dialect      dialect.Dialect
	valCreator valuer.Creator
}

func Open(driver string, dsn string, opts...OrmOption) (*Orm, error) {
	db, err := sql.Open(driver, dsn)
	if err != nil {
		return nil, err
	}
	dl, err := dialect.Of(driver)
	if err != nil {
		return nil, err
	}
	orm := &Orm{
		metaRegistry: model.NewMetaRegistry(),
		dialect:      dl,
		valCreator: valuer.NewUnsafeValue,
		db: db,
	}
	for _, o := range opts {
		o(orm)
	}
	return orm, nil
}

// WithMetaRegistry 指定元数据注册中心
func WithMetaRegistry(registry model.MetaRegistry) OrmOption {
	return func(db *Orm) {
		db.metaRegistry = registry
	}
}

// WithDialect 指定方言
func WithDialect(dialect dialect.Dialect) OrmOption {
	return func(db *Orm) {
		db.dialect = dialect
	}
}

// Delete 开始构建一个 DELETE 查询
func (o *Orm) Delete() *Deleter {
	return &Deleter{
		builder: o.builder(),
	}
}

// Update 开始构建一个 UPDATE 查询
func (o *Orm) Update(table interface{}) *Updater {
	return &Updater{
		builder: o.builder(),
		table:   table,
	}
}

// Wait 会等待数据库连接
// 注意只能用于测试
func (o *Orm) Wait() error {
	err := o.db.Ping()
	for err == driver.ErrBadConn {
		log.Printf("等待数据库启动...")
		err = o.db.Ping()
		time.Sleep(time.Second)
	}
	return err
}

func (o *Orm) builder() builder {
	return builder{
		registry:   o.metaRegistry,
		dialect:    o.dialect,
		buffer:     bytebufferpool.Get(),
		valCreator: o.valCreator,
	}
}