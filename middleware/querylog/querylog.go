package querylog

import (
	"context"
	"github.com/gotomicro/eorm"
	"log"
)

type MiddlewareBuilder struct {
	outputArgs bool
	logFunc    func(sql string, args ...any)
}

func NewBuilder() *MiddlewareBuilder {
	res := &MiddlewareBuilder{}
	res.logFunc = func(sql string, args ...any) {
		if res.outputArgs {
			log.Println(sql, args)
		} else {
			log.Println(sql)
		}
	}
	return res
}

func (b *MiddlewareBuilder) OutputArgs(outputArgs bool) *MiddlewareBuilder {
	b.outputArgs = outputArgs
	return b
}

func (b *MiddlewareBuilder) LogFunc(logFunc func(sql string, args ...any)) *MiddlewareBuilder {
	b.logFunc = logFunc
	return b
}

func (b *MiddlewareBuilder) Build() eorm.Middleware {
	return func(next eorm.HandleFunc) eorm.HandleFunc {
		return func(ctx context.Context, queryContext *eorm.QueryContext) *eorm.QueryResult {
			query := queryContext.GetQuery()
			if b.outputArgs {
				b.logFunc(query.SQL, query.Args...)
			} else {
				b.logFunc(query.SQL)
			}
			return next(ctx, queryContext)
		}
	}
}