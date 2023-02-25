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

package querylog

import (
	"context"
	"log"

	"github.com/ecodeclub/eorm"
)

type MiddlewareBuilder struct {
	logFunc func(sql string, args ...any)
}

func NewBuilder() *MiddlewareBuilder {
	return &MiddlewareBuilder{
		logFunc: func(sql string, args ...any) {
			log.Println(sql, args)
		},
	}

}

func (b *MiddlewareBuilder) LogFunc(logFunc func(sql string, args ...any)) *MiddlewareBuilder {
	b.logFunc = logFunc
	return b
}

func (b *MiddlewareBuilder) Build() eorm.Middleware {
	return func(next eorm.HandleFunc) eorm.HandleFunc {
		return func(ctx context.Context, queryContext *eorm.QueryContext) *eorm.QueryResult {
			query := queryContext.GetQuery()
			b.logFunc(query.SQL, query.Args...)
			return next(ctx, queryContext)
		}
	}
}
