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

	"github.com/ecodeclub/ekit/list"
	"github.com/ecodeclub/ekit/mapx"
	"github.com/ecodeclub/ekit/sqlx"
	"github.com/ecodeclub/eorm/internal/rows"
	"github.com/valyala/bytebufferpool"
	"golang.org/x/sync/errgroup"

	"github.com/ecodeclub/eorm/internal/datasource"
)

type Tx struct {
	baseSession
	tx datasource.Tx
}

func (t *Tx) queryMulti(ctx context.Context, qs []Query) (list.List[rows.Rows], error) {
	// 事务在查询的时候，需要将同一个 DB 上的语句合并在一起
	// 参考 https://github.com/ecodeclub/eorm/discussions/213
	mp := mapx.NewMultiBuiltinMap[string, Query](len(qs))
	for _, q := range qs {
		if err := mp.Put(q.DB+"_"+q.Datasource, q); err != nil {
			return nil, err
		}
	}
	keys := mp.Keys()
	rowsList := &list.ConcurrentList[rows.Rows]{
		List: list.NewArrayList[rows.Rows](len(keys)),
	}
	var eg errgroup.Group
	for _, key := range keys {
		dbQs, _ := mp.Get(key)
		eg.Go(func() error {
			return t.execDBQueries(ctx, dbQs, rowsList)
		})
	}
	return rowsList, eg.Wait()
}

// execDBQueries 执行某个 DB 上的全部查询。
// 执行结果会被加入进去 rowsList 里面。虽然这种修改传入参数的做法不是很好，但是作为一个内部方法还是可以接受的。
func (t *Tx) execDBQueries(ctx context.Context, dbQs []Query, rowsList *list.ConcurrentList[rows.Rows]) error {
	qsCnt := len(dbQs)
	// 考虑到大部分都只有一个查询，我们做一个快路径的优化。
	if qsCnt == 1 {
		rs, err := t.tx.Query(ctx, dbQs[0])
		if err != nil {
			return err
		}
		return rowsList.Append(rs)
	}
	// 慢路径，也就是必须要把同一个库的查询合并在一起
	q := t.mergeDBQueries(dbQs)
	rs, err := t.tx.Query(ctx, q)
	if err != nil {
		return err
	}
	// 查询之后，事务必须再次按照结果集分割开。
	// 这样是为了让结果集的数量和查询数量保持一致。
	return t.splitTxResultSet(rowsList, rs)
}

func (t *Tx) splitTxResultSet(list list.List[rows.Rows], rs *sql.Rows) error {
	cs, err := rs.Columns()
	if err != nil {
		return err
	}
	ct, err := rs.ColumnTypes()
	if err != nil {
		return err
	}
	scanner, err := sqlx.NewSQLRowsScanner(rs)
	if err != nil {
		return err
	}
	// 虽然这里我们可以尝试不读取最后一个 ResultSet
	// 但是这个优化目前来说不准备做，
	// 防止用户出现因为类型转换遇到一些潜在的问题
	// 数据库类型到 GO 类型再到用户希望的类型，是一个漫长的过程。
	hasNext := true
	for hasNext {
		var data [][]any
		data, err = scanner.ScanAll()
		if err != nil {
			return err
		}
		err = list.Append(rows.NewDataRows(data, cs, ct))
		if err != nil {
			return err
		}
		hasNext = scanner.NextResultSet()
	}
	return nil
}

func (t *Tx) mergeDBQueries(dbQs []Query) Query {
	buffer := bytebufferpool.Get()
	defer bytebufferpool.Put(buffer)
	first := dbQs[0]
	// 预估有多少查询参数，一个查询的参数个数 * 查询个数
	args := make([]any, 0, len(first.Args)*len(dbQs))
	for _, dbQ := range dbQs {
		_, _ = buffer.WriteString(dbQ.SQL)
		args = append(args, dbQ.Args...)
	}
	return Query{
		SQL:        buffer.String(),
		Args:       args,
		DB:         first.DB,
		Datasource: first.Datasource,
	}
}

func (t *Tx) Commit() error {
	return t.tx.Commit()
}

func (t *Tx) Rollback() error {
	return t.tx.Rollback()
}
