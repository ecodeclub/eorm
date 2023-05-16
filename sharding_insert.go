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
	"errors"
	"reflect"
	"strings"
	"sync"

	"github.com/ecodeclub/ekit/mapx"

	"github.com/ecodeclub/eorm/internal/errs"
	"github.com/ecodeclub/eorm/internal/model"
	"github.com/ecodeclub/eorm/internal/sharding"
	"github.com/valyala/bytebufferpool"
	"go.uber.org/multierr"
)

type ShardingInsert[T any] struct {
	builder
	columns  []string
	values   []*T
	ignorePK bool
	db       Session
	lock     sync.RWMutex
}

func (si *ShardingInsert[T]) Build(ctx context.Context) ([]sharding.Query, error) {
	defer bytebufferpool.Put(si.buffer)
	var err error
	if len(si.values) == 0 {
		return nil, errors.New("插入0行")
	}
	si.meta, err = si.metaRegistry.Get(si.values[0])
	if err != nil {
		return nil, err
	}
	colMetaData, err := si.getColumns()
	if err != nil {
		return nil, err
	}
	skNames := si.meta.ShardingAlgorithm.ShardingKeys()
	if err := si.checkColumns(colMetaData, skNames); err != nil {
		return nil, err
	}
	dsDBTabMap, err := mapx.NewTreeMap[key, []*T](compareDSDBTab)
	if err != nil {
		return nil, err
	}
	dsDBMap, err := mapx.NewTreeMap[key, []tableValue[T]](compareDSDB)
	if err != nil {
		return nil, err
	}
	for _, value := range si.values {
		dst, err := si.findDst(ctx, value)
		if err != nil {
			return nil, err
		}
		// 一个value只能命中一个库表如果不满足就报错
		if len(dst.Dsts) != 1 {
			return nil, errs.ErrInsertFindingDst
		}
		val, _ := dsDBTabMap.Get(key{dst.Dsts[0]})
		val = append(val, value)
		err = dsDBTabMap.Put(key{dst.Dsts[0]}, val)
		if err != nil {
			return nil, err
		}
		dsDBVal, ok := dsDBMap.Get(key{dst.Dsts[0]})
		if !ok {
			err = dsDBMap.Put(key{dst.Dsts[0]}, []tableValue[T]{{
				table:  dst.Dsts[0].Table,
				values: val,
			}})
			if err != nil {
				return nil, err
			}
		} else {
			flag := false
			for idx, tableVal := range dsDBVal {
				if tableVal.table == dst.Dsts[0].Table {
					dsDBVal[idx].values = val
					flag = true
					break
				}
			}
			if !flag {
				dsDBVal = append(dsDBVal, tableValue[T]{
					table:  dst.Dsts[0].Table,
					values: val,
				})
				err = dsDBMap.Put(key{dst.Dsts[0]}, dsDBVal)
				if err != nil {
					return nil, err
				}
			}
		}
	}

	// 针对每一个目标库，生成一个 insert 语句
	dsDBKeys := dsDBMap.Keys()
	ansQuery := make([]sharding.Query, 0, len(dsDBKeys))
	for _, dsDBKey := range dsDBKeys {
		ds := dsDBKey.Name
		db := dsDBKey.DB
		dsDBVal, _ := dsDBMap.Get(dsDBKey)
		for _, dsDBTabVal := range dsDBVal {
			err = si.buildQuery(ds, db, dsDBTabVal.table, colMetaData, dsDBTabVal.values)
			if err != nil {
				return nil, err
			}
		}
		ansQuery = append(ansQuery, sharding.Query{
			SQL:        si.buffer.String(),
			Args:       si.args,
			DB:         db,
			Datasource: ds,
		})
		si.buffer.Reset()
		si.args = []any{}
	}
	return ansQuery, nil
}

func (si *ShardingInsert[T]) buildQuery(ds, db, table string, colMetas []*model.ColumnMeta, values []*T) error {
	var err error
	si.writeString("INSERT INTO ")
	si.quote(db)
	si.writeByte('.')
	si.quote(table)
	si.writeString("(")
	err = si.buildColumns(colMetas)
	if err != nil {
		return err
	}
	si.writeString(")")
	si.writeString(" VALUES")
	for index, val := range values {
		if index > 0 {
			si.comma()
		}
		si.writeString("(")
		refVal := si.valCreator.NewPrimitiveValue(val, si.meta)
		for j, v := range colMetas {
			fdVal, err := refVal.Field(v.FieldName)
			if err != nil {
				return err
			}
			si.parameter(fdVal.Interface())
			if j != len(colMetas)-1 {
				si.comma()
			}
		}
		si.writeString(")")
	}
	si.end()
	return nil
}

// checkColumns 判断sk是否存在于meta中，如果不存在会返回报错
func (si *ShardingInsert[T]) checkColumns(colMetas []*model.ColumnMeta, sks []string) error {
	colMetasMap := make(map[string]struct{}, len(colMetas))
	for _, colMeta := range colMetas {
		colMetasMap[colMeta.FieldName] = struct{}{}
	}
	for _, sk := range sks {
		if _, ok := colMetasMap[sk]; !ok {
			return errs.ErrInsertShardingKeyNotFound
		}
	}
	return nil
}

func (si *ShardingInsert[T]) findDst(ctx context.Context, val *T) (sharding.Result, error) {
	sks := si.meta.ShardingAlgorithm.ShardingKeys()
	skValues := make(map[string]any)
	for _, sk := range sks {
		refVal := reflect.ValueOf(val).Elem().FieldByName(sk).Interface()
		skValues[sk] = refVal
	}
	return si.meta.ShardingAlgorithm.Sharding(ctx, sharding.Request{
		Op:       opEQ,
		SkValues: skValues,
	})
}

func (si *ShardingInsert[T]) getColumns() ([]*model.ColumnMeta, error) {
	cs := make([]*model.ColumnMeta, 0, len(si.columns))
	if len(si.columns) != 0 {
		for _, c := range si.columns {
			v, isOk := si.meta.FieldMap[c]
			if !isOk {
				return cs, errs.NewInvalidFieldError(c)
			}
			cs = append(cs, v)
		}
	} else {
		for _, val := range si.meta.Columns {
			if si.ignorePK && val.IsPrimaryKey {
				continue
			}
			cs = append(cs, val)
		}
	}
	return cs, nil
}

func (si *ShardingInsert[T]) buildColumns(colMetas []*model.ColumnMeta) error {
	for idx, colMeta := range colMetas {
		si.quote(colMeta.ColumnName)
		if idx != len(colMetas)-1 {
			si.comma()
		}
	}
	return nil
}

func (si *ShardingInsert[T]) Values(values []*T) *ShardingInsert[T] {
	si.values = values
	return si
}

func (si *ShardingInsert[T]) Columns(cols []string) *ShardingInsert[T] {
	si.columns = cols
	return si
}

func (si *ShardingInsert[T]) IgnorePK() *ShardingInsert[T] {
	si.ignorePK = true
	return si
}

func NewShardingInsert[T any](db Session) *ShardingInsert[T] {
	return &ShardingInsert[T]{
		db: db,
		builder: builder{
			core:   db.getCore(),
			buffer: bytebufferpool.Get(),
		},
		columns: []string{},
	}
}

func (si *ShardingInsert[T]) Exec(ctx context.Context) MultiExecRes {
	qs, err := si.Build(ctx)
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
				si.lock.Unlock()
				wg.Done()
			}()
			res, err := si.db.execContext(ctx, q)
			si.lock.Lock()
			errList[idx] = err
			resList[idx] = res
		}(idx, q)
	}
	wg.Wait()
	shardingRes := NewMultiExecRes(resList)
	shardingRes.err = multierr.Combine(errList...)
	return shardingRes
}

type key struct {
	sharding.Dst
}

func compareDSDBTab(i, j key) int {
	strI := strings.Join([]string{i.Name, i.DB, i.Table}, "")
	strJ := strings.Join([]string{j.Name, j.DB, j.Table}, "")
	if strI < strJ {
		return -1
	} else if strI == strJ {
		return 0
	}
	return 1

}

func compareDSDB(i, j key) int {
	strI := strings.Join([]string{i.Name, i.DB}, "")
	strJ := strings.Join([]string{j.Name, j.DB}, "")
	if strI < strJ {
		return -1
	} else if strI == strJ {
		return 0
	}
	return 1
}

type tableValue[T any] struct {
	table  string
	values []*T
}
