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
	"sync"

	"go.uber.org/multierr"

	"github.com/ecodeclub/eorm/internal/errs"
	"github.com/ecodeclub/eorm/internal/sharding"
	"github.com/valyala/bytebufferpool"
)

var _ sharding.Executor = &ShardingUpdater[any]{}

type ShardingUpdater[T any] struct {
	table *T
	lock  sync.Mutex
	db    Session
	shardingUpdaterBuilder
}

// NewShardingUpdater 开始构建一个 Sharding UPDATE 查询
func NewShardingUpdater[T any](sess Session) *ShardingUpdater[T] {
	b := shardingUpdaterBuilder{}
	b.core = sess.getCore()
	b.buffer = bytebufferpool.Get()
	return &ShardingUpdater[T]{
		shardingUpdaterBuilder: b,
		db:                     sess,
	}
}

func (s *ShardingUpdater[T]) Update(val *T) *ShardingUpdater[T] {
	s.table = val
	return s
}

func (s *ShardingUpdater[T]) Set(assigns ...Assignable) *ShardingUpdater[T] {
	s.assigns = assigns
	return s
}

func (s *ShardingUpdater[T]) Where(predicates ...Predicate) *ShardingUpdater[T] {
	s.where = predicates
	return s
}

// Build returns UPDATE []sharding.Query
func (s *ShardingUpdater[T]) Build(ctx context.Context) ([]sharding.Query, error) {
	if s.table == nil {
		s.table = new(T)
	}
	var err error
	if s.meta == nil {
		s.meta, err = s.metaRegistry.Get(s.table)
		if err != nil {
			return nil, err
		}
	}
	shardingRes, err := s.findDst(ctx, s.where...)
	if err != nil {
		return nil, err
	}

	res := make([]sharding.Query, 0, len(shardingRes.Dsts))
	defer bytebufferpool.Put(s.buffer)
	for _, dst := range shardingRes.Dsts {
		q, err := s.buildQuery(dst.DB, dst.Table, dst.Name)
		if err != nil {
			return nil, err
		}
		res = append(res, q)
		s.args = nil
		s.buffer.Reset()
	}
	return res, nil
}

func (s *ShardingUpdater[T]) buildQuery(db, tbl, ds string) (sharding.Query, error) {
	var err error

	s.val = s.valCreator.NewPrimitiveValue(s.table, s.meta)

	s.writeString("UPDATE ")
	s.quote(db)
	s.writeByte('.')
	s.quote(tbl)
	s.writeString(" SET ")
	if len(s.assigns) == 0 {
		err = s.buildDefaultColumns()
	} else {
		err = s.buildAssigns()
	}
	if err != nil {
		return sharding.EmptyQuery, err
	}

	if len(s.where) > 0 {
		s.writeString(" WHERE ")
		err = s.buildPredicates(s.where)
		if err != nil {
			return sharding.EmptyQuery, err
		}
	}
	s.end()

	return sharding.Query{SQL: s.buffer.String(), Args: s.args, Datasource: ds, DB: db}, nil
}

func (s *ShardingUpdater[T]) buildAssigns() error {
	has := false
	shardingKey := s.meta.ShardingAlgorithm.ShardingKeys()[0]
	for _, assign := range s.assigns {
		if has {
			s.comma()
		}
		switch a := assign.(type) {
		case Column:
			if a.name == shardingKey {
				return errs.NewErrUpdateShardingKeyUnsupported(a.name)
			}
			c, ok := s.meta.FieldMap[a.name]
			if !ok {
				return errs.NewInvalidFieldError(a.name)
			}
			refVal, err := s.val.Field(a.name)
			if err != nil {
				return err
			}
			s.quote(c.ColumnName)
			_ = s.buffer.WriteByte('=')
			s.parameter(refVal.Interface())
			has = true
		case columns:
			for _, name := range a.cs {
				if name == shardingKey {
					return errs.NewErrUpdateShardingKeyUnsupported(name)
				}
				c, ok := s.meta.FieldMap[name]
				if !ok {
					return errs.NewInvalidFieldError(name)
				}
				refVal, err := s.val.Field(name)
				if err != nil {
					return err
				}
				if has {
					s.comma()
				}
				s.quote(c.ColumnName)
				_ = s.buffer.WriteByte('=')
				s.parameter(refVal.Interface())
				has = true
			}
		case Assignment:
			if err := s.buildExpr(binaryExpr(a)); err != nil {
				return err
			}
			has = true
		default:
			return errs.ErrUnsupportedAssignment
		}
	}
	if !has {
		return errs.NewValueNotSetError()
	}
	return nil
}

func (s *ShardingUpdater[T]) buildDefaultColumns() error {
	has := false
	shardingKey := s.meta.ShardingAlgorithm.ShardingKeys()[0]
	for _, c := range s.meta.Columns {
		fieldName := c.FieldName
		if fieldName == shardingKey {
			continue
		}
		refVal, _ := s.val.Field(fieldName)
		if s.ignoreZeroVal && isZeroValue(refVal) {
			continue
		}
		if s.ignoreNilVal && isNilValue(refVal) {
			continue
		}
		if has {
			_ = s.buffer.WriteByte(',')
		}
		s.quote(c.ColumnName)
		_ = s.buffer.WriteByte('=')
		s.parameter(refVal.Interface())
		has = true
	}
	if !has {
		return errs.NewValueNotSetError()
	}
	return nil
}

func (s *ShardingUpdater[T]) SkipNilValue() *ShardingUpdater[T] {
	s.ignoreNilVal = true
	return s
}

func (s *ShardingUpdater[T]) SkipZeroValue() *ShardingUpdater[T] {
	s.ignoreZeroVal = true
	return s
}

func (s *ShardingUpdater[T]) Exec(ctx context.Context) sharding.Result {
	qs, err := s.Build(ctx)
	if err != nil {
		return sharding.NewResult(nil, err)
	}
	errList := make([]error, len(qs))
	resList := make([]sql.Result, len(qs))
	var wg sync.WaitGroup
	wg.Add(len(qs))
	for idx, q := range qs {
		go func(idx int, q Query) {
			defer wg.Done()
			res, err := s.db.execContext(ctx, q)
			s.lock.Lock()
			errList[idx] = err
			resList[idx] = res
			s.lock.Unlock()
		}(idx, q)
	}
	wg.Wait()
	shardingRes := sharding.NewResult(resList, multierr.Combine(errList...))
	return shardingRes
}
