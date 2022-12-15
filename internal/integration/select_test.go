// Copyright 2021 gotomicro
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

//go:build e2e

package integration

import (
	"context"
	"database/sql"
	"testing"

	"github.com/gotomicro/eorm"
	"github.com/gotomicro/eorm/internal/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type SelectTestSuite struct {
	Suite
	data *test.SimpleStruct
}

func (s *SelectTestSuite) SetupSuite() {
	s.Suite.SetupSuite()
	s.data = test.NewSimpleStruct(1)
	res := eorm.NewInserter[test.SimpleStruct](s.orm).Values(s.data).Exec(context.Background())
	if res.Err() != nil {
		s.T().Fatal(res.Err())
	}
}

func (s *SelectTestSuite) TearDownSuite() {
	res := eorm.RawQuery[any](s.orm, "DELETE FROM `simple_struct`").Exec(context.Background())
	if res.Err() != nil {
		s.T().Fatal(res.Err())
	}
}

func (s *SelectTestSuite) TestSelectorGet() {
	testCases := []struct {
		name    string
		s       *eorm.Selector[test.SimpleStruct]
		wantErr error
		wantRes *test.SimpleStruct
	}{
		{
			name: "not found",
			s: eorm.NewSelector[test.SimpleStruct](s.orm).
				From(&test.SimpleStruct{}).
				Where(eorm.C("Id").EQ(9)),
			wantErr: eorm.ErrNoRows,
		},
		{
			name: "found",
			s: eorm.NewSelector[test.SimpleStruct](s.orm).
				From(&test.SimpleStruct{}).
				Where(eorm.C("Id").EQ(1)),
			wantRes: s.data,
		},
	}

	for _, tc := range testCases {
		s.T().Run(tc.name, func(t *testing.T) {
			res, err := tc.s.Get(context.Background())
			assert.Equal(t, tc.wantErr, err)
			if err != nil {
				return
			}
			assert.Equal(t, tc.wantRes, res)
		})
	}
}

func (s *SelectTestSuite) TestSelectorGetBaseType() {
	testCases := []struct {
		name     string
		queryRes func() (any, error)
		wantErr  error
		wantRes  any
	}{
		{
			name: "not found",
			queryRes: func() (any, error) {
				queryer := eorm.NewSelector[test.SimpleStruct](s.orm).
					Where(eorm.C("Id").EQ(9))
				return queryer.Get(context.Background())
			},
			wantErr: eorm.ErrNoRows,
		},
		{
			name: "res int",
			queryRes: func() (any, error) {
				queryer := eorm.NewSelector[int](s.orm).Select(eorm.C("Id")).
					From(&test.SimpleStruct{}).Where(eorm.C("Id").EQ(1))
				return queryer.Get(context.Background())
			},
			wantRes: func() *int {
				res := 1
				return &res
			}(),
		},
		{
			name: "res string",
			queryRes: func() (any, error) {
				queryer := eorm.NewSelector[string](s.orm).Select(eorm.C("String")).
					From(&test.SimpleStruct{}).Where(eorm.C("Id").EQ(1))
				return queryer.Get(context.Background())
			},
			wantRes: func() *string {
				res := "world"
				return &res
			}(),
		},
		{
			name: "res bytes",
			queryRes: func() (any, error) {
				queryer := eorm.NewSelector[[]byte](s.orm).Select(eorm.C("ByteArray")).
					From(&test.SimpleStruct{}).Where(eorm.C("Id").EQ(1))
				return queryer.Get(context.Background())
			},
			wantRes: func() *[]byte {
				res := []byte("hello")
				return &res
			}(),
		},
		{
			name: "res bool",
			queryRes: func() (any, error) {
				queryer := eorm.NewSelector[bool](s.orm).Select(eorm.C("Bool")).
					From(&test.SimpleStruct{}).Where(eorm.C("Id").EQ(1))
				return queryer.Get(context.Background())
			},
			wantRes: func() *bool {
				res := true
				return &res
			}(),
		},
		{
			name: "res null string ptr",
			queryRes: func() (any, error) {
				queryer := eorm.NewSelector[sql.NullString](s.orm).Select(eorm.C("NullStringPtr")).
					From(&test.SimpleStruct{}).Where(eorm.C("Id").EQ(1))
				return queryer.Get(context.Background())
			},
			wantRes: func() *sql.NullString {
				res := sql.NullString{String: "null string", Valid: true}
				return &res
			}(),
		},
		{
			name: "res null int32 ptr",
			queryRes: func() (any, error) {
				queryer := eorm.NewSelector[sql.NullInt32](s.orm).Select(eorm.C("NullInt32Ptr")).
					From(&test.SimpleStruct{}).Where(eorm.C("Id").EQ(1))
				return queryer.Get(context.Background())
			},
			wantRes: func() *sql.NullInt32 {
				res := sql.NullInt32{Int32: 32, Valid: true}
				return &res
			}(),
		},
		{
			name: "res null bool ptr",
			queryRes: func() (any, error) {
				queryer := eorm.NewSelector[sql.NullBool](s.orm).Select(eorm.C("NullBoolPtr")).
					From(&test.SimpleStruct{}).Where(eorm.C("Id").EQ(1))
				return queryer.Get(context.Background())
			},
			wantRes: func() *sql.NullBool {
				res := sql.NullBool{Bool: true, Valid: true}
				return &res
			}(),
		},
		{
			name: "res null float64 ptr",
			queryRes: func() (any, error) {
				queryer := eorm.NewSelector[sql.NullFloat64](s.orm).Select(eorm.C("NullFloat64Ptr")).
					From(&test.SimpleStruct{}).Where(eorm.C("Id").EQ(1))
				return queryer.Get(context.Background())
			},
			wantRes: func() *sql.NullFloat64 {
				res := sql.NullFloat64{Float64: 6.4, Valid: true}
				return &res
			}(),
		},
	}

	for _, tc := range testCases {
		s.T().Run(tc.name, func(t *testing.T) {
			res, err := tc.queryRes()
			assert.Equal(t, tc.wantErr, err)
			if err != nil {
				return
			}
			assert.Equal(t, tc.wantRes, res)
		})
	}
}

func (s *SelectTestSuite) TestRawQueryGetBaseType() {
	testCases := []struct {
		name     string
		queryRes func() (any, error)
		wantErr  error
		wantRes  any
	}{
		{
			name: "res int",
			queryRes: func() (any, error) {
				queryer := eorm.RawQuery[int](s.orm, "SELECT `id` FROM `simple_struct` WHERE `id` = ?;", 1)
				return queryer.Get(context.Background())
			},
			wantRes: func() *int {
				res := 1
				return &res
			}(),
		},
		{
			name: "res int convert string",
			queryRes: func() (any, error) {
				queryer := eorm.RawQuery[string](s.orm, "SELECT `id` FROM `simple_struct` WHERE `id` = ?;", 1)
				return queryer.Get(context.Background())
			},
			wantRes: func() *string {
				res := "1"
				return &res
			}(),
		},
		{
			name: "res int convert bytes",
			queryRes: func() (any, error) {
				queryer := eorm.RawQuery[[]byte](s.orm, "SELECT `id` FROM `simple_struct` WHERE `id` = ?;", 1)
				return queryer.Get(context.Background())
			},
			wantRes: func() *[]byte {
				res := []byte("1")
				return &res
			}(),
		},
		{
			name: "res string",
			queryRes: func() (any, error) {
				queryer := eorm.RawQuery[string](s.orm, "SELECT `string` FROM `simple_struct` WHERE `id` = ?;", 1)
				return queryer.Get(context.Background())
			},
			wantRes: func() *string {
				res := "world"
				return &res
			}(),
		},
		{
			name: "res string  convert bytes",
			queryRes: func() (any, error) {
				queryer := eorm.RawQuery[[]byte](s.orm, "SELECT `string` FROM `simple_struct` WHERE `id` = ?;", 1)
				return queryer.Get(context.Background())
			},
			wantRes: func() *[]byte {
				res := []byte("world")
				return &res
			}(),
		},
		{
			name: "res bytes",
			queryRes: func() (any, error) {
				queryer := eorm.RawQuery[[]byte](s.orm, "SELECT `byte_array` FROM `simple_struct` WHERE `id` = ?;", 1)
				return queryer.Get(context.Background())
			},
			wantRes: func() *[]byte {
				res := []byte("hello")
				return &res
			}(),
		},
		{
			name: "res bytes convert string",
			queryRes: func() (any, error) {
				queryer := eorm.RawQuery[string](s.orm, "SELECT `byte_array` FROM `simple_struct` WHERE `id` = ?;", 1)
				return queryer.Get(context.Background())
			},
			wantRes: func() *string {
				res := "hello"
				return &res
			}(),
		},
		{
			name: "res bool",
			queryRes: func() (any, error) {
				queryer := eorm.RawQuery[bool](s.orm, "SELECT `bool` FROM `simple_struct` WHERE `id` = ?;", 1)
				return queryer.Get(context.Background())
			},
			wantRes: func() *bool {
				res := true
				return &res
			}(),
		},
		{
			name: "res bool convert string",
			queryRes: func() (any, error) {
				queryer := eorm.RawQuery[string](s.orm, "SELECT `bool` FROM `simple_struct` WHERE `id` = ?;", 1)
				return queryer.Get(context.Background())
			},
			wantRes: func() *string {
				res := "1"
				return &res
			}(),
		},
		{
			name: "res bool convert in",
			queryRes: func() (any, error) {
				queryer := eorm.RawQuery[int](s.orm, "SELECT `bool` FROM `simple_struct` WHERE `id` = ?;", 1)
				return queryer.Get(context.Background())
			},
			wantRes: func() *int {
				res := 1
				return &res
			}(),
		},
		{
			name: "res null string ptr",
			queryRes: func() (any, error) {
				queryer := eorm.RawQuery[sql.NullString](s.orm, "SELECT `null_string_ptr` FROM `simple_struct` WHERE `id` = ?;", 1)
				return queryer.Get(context.Background())
			},
			wantRes: func() *sql.NullString {
				res := sql.NullString{String: "null string", Valid: true}
				return &res
			}(),
		},
		{
			name: "res sring convert null string ptr",
			queryRes: func() (any, error) {
				queryer := eorm.RawQuery[sql.NullString](s.orm, "SELECT `string` FROM `simple_struct` WHERE `id` = ?;", 1)
				return queryer.Get(context.Background())
			},
			wantRes: func() *sql.NullString {
				res := sql.NullString{String: "world", Valid: true}
				return &res
			}(),
		},
	}

	for _, tc := range testCases {
		s.T().Run(tc.name, func(t *testing.T) {
			res, err := tc.queryRes()
			assert.Equal(t, tc.wantErr, err)
			if err != nil {
				return
			}
			assert.Equal(t, tc.wantRes, res)
		})
	}
}

func TestMySQL8Select(t *testing.T) {
	suite.Run(t, &SelectTestSuite{
		Suite: Suite{
			driver: "mysql",
			dsn:    "root:root@tcp(localhost:13306)/integration_test",
		},
	})
	suite.Run(t, &SelectTestSuiteGetMulti{
		Suite: Suite{
			driver: "mysql",
			dsn:    "root:root@tcp(localhost:13306)/integration_test",
		},
	})
}

type SelectTestSuiteGetMulti struct {
	Suite
	data []*test.SimpleStruct
}

func (s *SelectTestSuiteGetMulti) SetupSuite() {
	s.Suite.SetupSuite()
	s.data = append(s.data, test.NewSimpleStruct(1))
	s.data = append(s.data, test.NewSimpleStruct(2))
	s.data = append(s.data, test.NewSimpleStruct(3))
	res := eorm.NewInserter[test.SimpleStruct](s.orm).Values(s.data...).Exec(context.Background())
	if res.Err() != nil {
		s.T().Fatal(res.Err())
	}
}

func (s *SelectTestSuiteGetMulti) TearDownSuite() {
	res := eorm.RawQuery[any](s.orm, "DELETE FROM `simple_struct`").Exec(context.Background())
	if res.Err() != nil {
		s.T().Fatal(res.Err())
	}
}

func (s *SelectTestSuiteGetMulti) TestSelectorGetMulti() {
	testCases := []struct {
		name    string
		s       *eorm.Selector[test.SimpleStruct]
		wantErr error
		wantRes []*test.SimpleStruct
	}{
		{
			name: "not found",
			s: eorm.NewSelector[test.SimpleStruct](s.orm).
				From(&test.SimpleStruct{}).
				Where(eorm.C("Id").EQ(9)),
			wantRes: []*test.SimpleStruct{},
		},
		{
			name: "found",
			s: eorm.NewSelector[test.SimpleStruct](s.orm).
				From(&test.SimpleStruct{}).
				Where(eorm.C("Id").LT(4)),
			wantRes: s.data,
		},
	}

	for _, tc := range testCases {
		s.T().Run(tc.name, func(t *testing.T) {
			res, err := tc.s.GetMulti(context.Background())
			assert.Equal(t, tc.wantErr, err)
			if err != nil {
				return
			}
			assert.Equal(t, tc.wantRes, res)
		})
	}
}

func (s *SelectTestSuiteGetMulti) TestSelectorGetMultiBaseType() {
	testCases := []struct {
		name     string
		queryRes func() (any, error)
		wantErr  error
		wantRes  any
	}{
		{
			name: "res int",
			queryRes: func() (any, error) {
				queryer := eorm.NewSelector[int](s.orm).Select(eorm.C("Id")).
					From(&test.SimpleStruct{})
				return queryer.GetMulti(context.Background())
			},
			wantRes: func() (res []*int) {
				vals := []int{1, 2, 3}
				for i := 0; i < len(vals); i++ {
					res = append(res, &vals[i])
				}
				return
			}(),
		},
		{
			name: "res string",
			queryRes: func() (any, error) {
				queryer := eorm.NewSelector[string](s.orm).Select(eorm.C("String")).
					From(&test.SimpleStruct{})
				return queryer.GetMulti(context.Background())
			},
			wantRes: func() (res []*string) {
				vals := []string{"world", "world", "world"}
				for i := 0; i < len(vals); i++ {
					res = append(res, &vals[i])
				}
				return
			}(),
		},
		{
			name: "res bytes",
			queryRes: func() (any, error) {
				queryer := eorm.NewSelector[[]byte](s.orm).Select(eorm.C("ByteArray")).
					From(&test.SimpleStruct{})
				return queryer.GetMulti(context.Background())
			},
			wantRes: func() (res []*[]byte) {
				vals := [][]byte{[]byte("hello"), []byte("hello"), []byte("hello")}
				for i := 0; i < len(vals); i++ {
					res = append(res, &vals[i])
				}
				return
			}(),
		},
		{
			name: "res bool",
			queryRes: func() (any, error) {
				queryer := eorm.NewSelector[bool](s.orm).Select(eorm.C("Bool")).
					From(&test.SimpleStruct{})
				return queryer.GetMulti(context.Background())
			},
			wantRes: func() (res []*bool) {
				vals := []bool{true, true, true}
				for i := 0; i < len(vals); i++ {
					res = append(res, &vals[i])
				}
				return
			}(),
		},
		{
			name: "res null string ptr",
			queryRes: func() (any, error) {
				queryer := eorm.NewSelector[sql.NullString](s.orm).Select(eorm.C("NullStringPtr")).
					From(&test.SimpleStruct{})
				return queryer.GetMulti(context.Background())
			},
			wantRes: []*sql.NullString{
				{
					String: "null string",
					Valid:  true,
				},
				{
					String: "null string",
					Valid:  true,
				},
				{
					String: "null string",
					Valid:  true,
				},
			},
		},
		{
			name: "res null int32 ptr",
			queryRes: func() (any, error) {
				queryer := eorm.NewSelector[sql.NullInt32](s.orm).Select(eorm.C("NullInt32Ptr")).
					From(&test.SimpleStruct{})
				return queryer.GetMulti(context.Background())
			},
			wantRes: []*sql.NullInt32{
				{
					Int32: 32,
					Valid: true,
				},
				{
					Int32: 32,
					Valid: true,
				},
				{
					Int32: 32,
					Valid: true,
				},
			},
		},
		{
			name: "res null bool ptr",
			queryRes: func() (any, error) {
				queryer := eorm.NewSelector[sql.NullBool](s.orm).Select(eorm.C("NullBoolPtr")).
					From(&test.SimpleStruct{})
				return queryer.GetMulti(context.Background())
			},
			wantRes: []*sql.NullBool{
				{
					Bool:  true,
					Valid: true,
				},
				{
					Bool:  true,
					Valid: true,
				},
				{
					Bool:  true,
					Valid: true,
				},
			},
		},
		{
			name: "res null float64 ptr",
			queryRes: func() (any, error) {
				queryer := eorm.NewSelector[sql.NullFloat64](s.orm).Select(eorm.C("NullFloat64Ptr")).
					From(&test.SimpleStruct{})
				return queryer.GetMulti(context.Background())
			},
			wantRes: []*sql.NullFloat64{
				{
					Float64: 6.4,
					Valid:   true,
				},
				{
					Float64: 6.4,
					Valid:   true,
				},
				{
					Float64: 6.4,
					Valid:   true,
				},
			},
		},
	}

	for _, tc := range testCases {
		s.T().Run(tc.name, func(t *testing.T) {
			res, err := tc.queryRes()
			assert.Equal(t, tc.wantErr, err)
			if err != nil {
				return
			}
			assert.EqualValues(t, tc.wantRes, res)
		})
	}
}

func (s *SelectTestSuiteGetMulti) TestRawQueryGetMultiBaseType() {
	testCases := []struct {
		name     string
		queryRes func() (any, error)
		wantErr  error
		wantRes  any
	}{
		{
			name: "res int",
			queryRes: func() (any, error) {
				queryer := eorm.RawQuery[int](s.orm, "SELECT `id` FROM `simple_struct`;")
				return queryer.GetMulti(context.Background())
			},
			wantRes: func() (res []*int) {
				vals := []int{1, 2, 3}
				for i := 0; i < len(vals); i++ {
					res = append(res, &vals[i])
				}
				return
			}(),
		},
		{
			name: "res string",
			queryRes: func() (any, error) {
				queryer := eorm.RawQuery[string](s.orm, "SELECT `string` FROM `simple_struct`;")
				return queryer.GetMulti(context.Background())
			},
			wantRes: func() (res []*string) {
				vals := []string{"world", "world", "world"}
				for i := 0; i < len(vals); i++ {
					res = append(res, &vals[i])
				}
				return
			}(),
		},
		{
			name: "res bytes",
			queryRes: func() (any, error) {
				queryer := eorm.RawQuery[[]byte](s.orm, "SELECT `byte_array` FROM `simple_struct`;")
				return queryer.GetMulti(context.Background())
			},
			wantRes: func() (res []*[]byte) {
				vals := [][]byte{[]byte("hello"), []byte("hello"), []byte("hello")}
				for i := 0; i < len(vals); i++ {
					res = append(res, &vals[i])
				}
				return
			}(),
		},
		{
			name: "res bool",
			queryRes: func() (any, error) {
				queryer := eorm.RawQuery[bool](s.orm, "SELECT `bool` FROM `simple_struct`;")
				return queryer.GetMulti(context.Background())
			},
			wantRes: func() (res []*bool) {
				vals := []bool{true, true, true}
				for i := 0; i < len(vals); i++ {
					res = append(res, &vals[i])
				}
				return
			}(),
		},
		{
			name: "res null string ptr",
			queryRes: func() (any, error) {
				queryer := eorm.RawQuery[sql.NullString](s.orm, "SELECT `null_string_ptr` FROM `simple_struct`;")
				return queryer.GetMulti(context.Background())
			},
			wantRes: []*sql.NullString{
				{
					String: "null string",
					Valid:  true,
				},
				{
					String: "null string",
					Valid:  true,
				},
				{
					String: "null string",
					Valid:  true,
				},
			},
		},
	}

	for _, tc := range testCases {
		s.T().Run(tc.name, func(t *testing.T) {
			res, err := tc.queryRes()
			assert.Equal(t, tc.wantErr, err)
			if err != nil {
				return
			}
			assert.EqualValues(t, tc.wantRes, res)
		})
	}
}

func (s *SelectTestSuiteGetMulti) TestSelectorDistinct() {

	testcases := []struct {
		name    string
		s       func() (any, error)
		wantErr error
		wantRes any
	}{
		{
			name: "distinct col",
			s: func() (any, error) {
				return eorm.NewSelector[test.SimpleStruct](s.orm).From(&test.SimpleStruct{}).Select(eorm.C("Int")).Distinct().GetMulti(context.Background())

			},
			wantRes: []*test.SimpleStruct{
				&test.SimpleStruct{
					Int: 12,
				},
			},
		},
		{
			name: "count distinct",
			s: func() (any, error) {
				return eorm.NewSelector[int](s.orm).Select(eorm.CountDistinct("Bool")).From(&test.SimpleStruct{}).GetMulti(context.Background())
			},
			wantRes: func() []*int {
				val := 1
				return []*int{&val}
			}(),
		},
		{
			name: "having count distinct",
			s: func() (any, error) {
				return eorm.NewSelector[test.SimpleStruct](s.orm).From(&test.SimpleStruct{}).Select(eorm.C("JsonColumn")).GroupBy("JsonColumn").Having(eorm.CountDistinct("JsonColumn").EQ(1)).GetMulti(context.Background())
			},
			wantRes: []*test.SimpleStruct{
				&test.SimpleStruct{
					JsonColumn: &test.JsonColumn{
						Val:   test.User{Name: "Tom"},
						Valid: true,
					},
				},
			},
		},
	}
	for _, tc := range testcases {
		s.T().Run(tc.name, func(t *testing.T) {
			res, err := tc.s()
			assert.Equal(t, tc.wantErr, err)
			if err != nil {
				return
			}
			assert.Equal(t, tc.wantRes, res)
		})
	}

}
