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

package transaction

import (
	"context"
	"database/sql"
	"errors"

	"github.com/ecodeclub/eorm/internal/datasource"
)

// 为了方便管理不同类型的 分布式 Tx，所以这里引入 TxType 常量来支持创建不同的 分布式Tx类型 以便提高后续引入 XA 方案的扩展性。
const (
	Delay    = "delay"
	BinMulti = "binMulti"
)

type TxFactory interface {
	TxOf(ctx Context, b map[string]datasource.TxBeginner) (datasource.Tx, error)
}

type Context struct {
	TxName string
	TxCtx  context.Context
	Opts   *sql.TxOptions
}

type TypeKey struct{}

func UsingTxType(ctx context.Context, val string) context.Context {
	return context.WithValue(ctx, TypeKey{}, val)
}

func GetCtxTypeKey(ctx context.Context) any {
	return ctx.Value(TypeKey{})
}

type TxFacade struct {
	factory   TxFactory
	ctx       Context
	beginners map[string]datasource.TxBeginner
}

func NewTxFacade(ctx context.Context, beginners map[string]datasource.TxBeginner) (TxFacade, error) {
	res := TxFacade{
		beginners: beginners,
	}
	switch GetCtxTypeKey(ctx).(string) {
	case Delay:
		res.factory = DelayTxFactory{}
		return res, nil
	case BinMulti:
		res.factory = BinMultiTxFactory{}
		return res, nil
	default:
		return TxFacade{}, errors.New("不支持的分布式事务类型")
	}
}

func (t *TxFacade) BeginTx(ctx context.Context, opts *sql.TxOptions) (datasource.Tx, error) {
	dsCtx := Context{
		TxCtx:  ctx,
		Opts:   opts,
		TxName: GetCtxTypeKey(ctx).(string),
	}

	return t.factory.TxOf(dsCtx, t.beginners)
}
