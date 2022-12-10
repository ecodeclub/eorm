package eorm

import (
	"context"

	"github.com/gotomicro/eorm/internal/model"
)

type QueryContext struct {
	Type    string
	meta    *model.TableMeta
	Builder QueryBuilder
	q       *Query
}

func (qc *QueryContext) Query() (*Query, error) {
	if qc.q != nil {
		return qc.q, nil
	}
	return qc.Builder.Build()
}

type QueryResult struct {
	Result any
	Err    error
}

type Middleware func(next HandleFunc) HandleFunc

type HandleFunc func(ctx context.Context, queryContext *QueryContext) *QueryResult
