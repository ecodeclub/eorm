package querylog

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/gotomicro/eorm"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestMiddlewareBuilder_Build(t *testing.T) {
	testCases := []struct {
		name    string
		builder *MiddlewareBuilder
		wantVal any
	}{
		{
			name:    "not args",
			builder: NewBuilder(),
		},
		{
			name:    "output args",
			builder: NewBuilder().OutputArgs(true),
		},
		{
			name: "log func",
			builder: func() *MiddlewareBuilder {
				builder := NewBuilder()
				builder.LogFunc(func(sql string, args ...any) {
					fmt.Println(sql)
				})
				return builder
			}(),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			db, err := eorm.Open("sqlite3",
				"file:test.db?cache=shared&mode=memory", eorm.DBWithMiddlewares(
					tc.builder.Build()))
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				_ = db.Close()
			}()
			_, err = eorm.NewSelector[TestModel](db).Get(context.Background())
			assert.NotNil(t, err)
		})
	}

}

type TestModel struct {
	Id        int64 `eorm:"auto_increment,primary_key"`
	FirstName string
	Age       int8
	LastName  *sql.NullString
}
