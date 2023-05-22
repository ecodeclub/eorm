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
	"fmt"
	"sync"

	"go.uber.org/multierr"

	"github.com/ecodeclub/ekit/mapx"
	"github.com/ecodeclub/eorm/internal/errs"
	"github.com/ecodeclub/eorm/internal/sharding"
	"github.com/valyala/bytebufferpool"
)

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
	//t := new(T)
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

	shardingRes, err := s.findDst(ctx)
	if err != nil {
		return nil, err
	}

	dsDBMap, err := mapx.NewTreeMap[sharding.Dst, *mapx.TreeMap[sharding.Dst, []*T]](sharding.CompareDSDB)
	if err != nil {
		return nil, err
	}

	for _, dst := range shardingRes.Dsts {
		dsDBVal, ok := dsDBMap.Get(dst)
		if !ok {
			dsDBVal, err = mapx.NewTreeMap[sharding.Dst, []*T](sharding.CompareDSDBTab)
			if err != nil {
				return nil, err
			}
			err = dsDBVal.Put(dst, []*T{s.table})
			if err != nil {
				return nil, err
			}
		} else {
			valList, _ := dsDBVal.Get(dst)
			valList = append(valList, s.table)
			err = dsDBVal.Put(dst, valList)
			if err != nil {
				return nil, err
			}
		}
		err = dsDBMap.Put(dst, dsDBVal)
		if err != nil {
			return nil, err
		}
	}

	// 针对每一个目标库，生成一个 update 语句
	dsDBKeys := dsDBMap.Keys()
	res := make([]sharding.Query, 0, len(dsDBKeys))
	defer bytebufferpool.Put(s.buffer)
	for _, dsDBKey := range dsDBKeys {
		ds := dsDBKey.Name
		db := dsDBKey.DB
		dsDBVal, _ := dsDBMap.Get(dsDBKey)
		for _, dsDBTabKey := range dsDBVal.Keys() {
			err = s.buildQuery(db, dsDBTabKey.Table)
			if err != nil {
				return nil, err
			}
		}
		res = append(res, sharding.Query{
			SQL:        s.buffer.String(),
			Args:       s.args,
			DB:         db,
			Datasource: ds,
		})
		s.args = nil
		s.buffer.Reset()
	}

	return res, nil

}

func (s *ShardingUpdater[T]) buildQuery(db, tbl string) error {
	var err error

	s.val = s.valCreator.NewPrimitiveValue(s.table, s.meta)
	s.args = make([]interface{}, 0, len(s.meta.Columns))

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
		return err
	}

	if len(s.where) > 0 {
		s.writeString(" WHERE ")
		err = s.buildPredicates(s.where)
		if err != nil {
			return err
		}
	}
	s.end()

	return nil
}

func (s *ShardingUpdater[T]) findDst(ctx context.Context) (sharding.Result, error) {
	if len(s.where) > 0 {
		pre := s.where[0]
		for i := 1; i < len(s.where)-1; i++ {
			pre = pre.And(s.where[i])
		}
		return s.findDstByPredicate(ctx, pre)
	}
	res := sharding.Result{
		Dsts: s.meta.ShardingAlgorithm.Broadcast(ctx),
	}
	return res, nil
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
				return errs.ErrUpdateShardingKeyUnsupported
			}
			c, ok := s.meta.FieldMap[a.name]
			if !ok {
				return errs.NewInvalidFieldError(a.name)
			}
			refVal, _ := s.val.Field(a.name)
			s.quote(c.ColumnName)
			_ = s.buffer.WriteByte('=')
			s.parameter(refVal.Interface())
			has = true
		case columns:
			for _, name := range a.cs {
				if name == shardingKey {
					return errs.ErrUpdateShardingKeyUnsupported
				}
				c, ok := s.meta.FieldMap[name]
				if !ok {
					return errs.NewInvalidFieldError(name)
				}
				refVal, _ := s.val.Field(name)
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
			return fmt.Errorf("eorm: unsupported assignment %v", a)
		}
	}
	if !has {
		return errs.NewValueNotSetError()
	}
	return nil
}

// 如果不允许修改 sk， 是否就可以强制用户输入目标列，从而舍弃 buildDefaultColumns ？？
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

func (s *ShardingUpdater[T]) Exec(ctx context.Context) MultiExecRes {
	qs, err := s.Build(ctx)
	if err != nil {
		return MultiExecRes{
			err: err,
		}
	}
	errList := make([]error, len(qs))
	resList := make([]sql.Result, len(qs))
	var wg sync.WaitGroup
	wg.Add(len(qs))
	for idx, q := range qs {
		go func(idx int, q Query) {
			defer func() {
				s.lock.Unlock()
				wg.Done()
			}()
			res, err := s.db.execContext(ctx, q)
			s.lock.Lock()
			errList[idx] = err
			resList[idx] = res
		}(idx, q)
	}
	wg.Wait()
	shardingRes := NewMultiExecRes(resList)
	shardingRes.err = multierr.Combine(errList...)
	return shardingRes
}
