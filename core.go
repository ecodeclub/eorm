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
	"reflect"

	"github.com/ecodeclub/eorm/internal/dialect"
	"github.com/ecodeclub/eorm/internal/errs"
	"github.com/ecodeclub/eorm/internal/model"
	"github.com/ecodeclub/eorm/internal/valuer"
)

type core struct {
	metaRegistry model.MetaRegistry
	dialect      dialect.Dialect
	valCreator   valuer.PrimitiveCreator
	ms           []Middleware
}

func getHandler[T any](ctx context.Context, sess Session, c core, qc *QueryContext) *QueryResult {
	rows, err := sess.queryContext(ctx, qc.q)
	if err != nil {
		return &QueryResult{Err: err}
	}
	defer func() {
		_ = rows.Close()
	}()
	if !rows.Next() {
		return &QueryResult{Err: errs.ErrNoRows}
	}

	tp := new(T)
	meta := qc.meta
	if meta == nil && reflect.TypeOf(tp).Elem().Kind() == reflect.Struct {
		//  当通过 RawQuery 方法调用 Get ,如果 T 是 time.Time, sql.Scanner 的实现，
		//  内置类型或者基本类型时， 在这里都会报错，但是这种情况我们认为是可以接受的
		//  所以在此将报错忽略，因为基本类型取值用不到 meta 里的数据
		meta, _ = c.metaRegistry.Get(tp)
	}

	val := c.valCreator.NewPrimitiveValue(tp, meta)
	if err = val.SetColumns(rows); err != nil {
		return &QueryResult{Err: err}
	}

	return &QueryResult{Result: tp}
}

func get[T any](ctx context.Context, sess Session, core core, qc *QueryContext) *QueryResult {
	var handler HandleFunc = func(ctx context.Context, queryContext *QueryContext) *QueryResult {
		return getHandler[T](ctx, sess, core, queryContext)
	}
	ms := core.ms
	for i := len(ms) - 1; i >= 0; i-- {
		handler = ms[i](handler)
	}
	return handler(ctx, qc)
}

func getMultiHandler[T any](ctx context.Context, sess Session, c core, qc *QueryContext) *QueryResult {
	rows, err := sess.queryContext(ctx, qc.q)
	if err != nil {
		return &QueryResult{Err: err}
	}
	defer func() {
		_ = rows.Close()
	}()
	res := make([]*T, 0, 16)
	meta := qc.meta
	if meta == nil {
		t := new(T)
		if reflect.TypeOf(t).Elem().Kind() == reflect.Struct {
			//  当通过 RawQuery 方法调用 Get ,如果 T 是 time.Time, sql.Scanner 的实现，
			//  内置类型或者基本类型时， 在这里都会报错，但是这种情况我们认为是可以接受的
			//  所以在此将报错忽略，因为基本类型取值用不到 meta 里的数据
			meta, _ = c.metaRegistry.Get(t)
		}
	}
	for rows.Next() {
		tp := new(T)
		val := c.valCreator.NewPrimitiveValue(tp, meta)
		if err = val.SetColumns(rows); err != nil {
			return &QueryResult{Err: err}
		}
		res = append(res, tp)
	}

	return &QueryResult{Result: res}
}

func getMulti[T any](ctx context.Context, sess Session, core core, qc *QueryContext) *QueryResult {
	var handler HandleFunc = func(ctx context.Context, queryContext *QueryContext) *QueryResult {
		return getMultiHandler[T](ctx, sess, core, queryContext)
	}
	ms := core.ms
	for i := len(ms) - 1; i >= 0; i-- {
		handler = ms[i](handler)
	}
	return handler(ctx, qc)
}
