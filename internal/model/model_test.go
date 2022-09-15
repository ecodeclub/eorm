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

package model

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/gotomicro/eorm/internal/errs"
	"github.com/stretchr/testify/assert"
)

func TestTagMetaRegistry(t *testing.T) {

	testCases := []struct {
		name     string
		wantMeta *TableMeta
		wantErr  error
		input    interface{}
	}{
		{
			// 普通
			name: "Normal",
			wantMeta: &TableMeta{
				TableName: "test_model",
				Columns: []*ColumnMeta{
					{
						ColumnName:      "id",
						FieldName:       "Id",
						Typ:             reflect.TypeOf(int64(0)),
						IsPrimaryKey:    true,
						IsAutoIncrement: true,
						Ancestors:       []string{"TestModel"},
					},
					{
						ColumnName: "first_name",
						FieldName:  "FirstName",
						Typ:        reflect.TypeOf(""),
						Offset:     8,
						Ancestors:  []string{"TestModel"},
					},
					{
						ColumnName: "age",
						FieldName:  "Age",
						Typ:        reflect.TypeOf(int8(0)),
						Offset:     24,
						Ancestors:  []string{"TestModel"},
					},
					{
						ColumnName: "last_name",
						FieldName:  "LastName",
						Typ:        reflect.TypeOf(""),
						Offset:     32,
						Ancestors:  []string{"TestModel"},
					},
				},
				FieldMap: map[string]*ColumnMeta{
					"Id": {
						ColumnName:      "id",
						FieldName:       "Id",
						Typ:             reflect.TypeOf(int64(0)),
						IsPrimaryKey:    true,
						IsAutoIncrement: true,
						Ancestors:       []string{"TestModel"},
					},
					"FirstName": {
						ColumnName: "first_name",
						FieldName:  "FirstName",
						Typ:        reflect.TypeOf(""),
						Offset:     8,
						Ancestors:  []string{"TestModel"},
					},
					"Age": {
						ColumnName: "age",
						FieldName:  "Age",
						Typ:        reflect.TypeOf(int8(0)),
						Offset:     24,
						Ancestors:  []string{"TestModel"},
					},
					"LastName": {
						ColumnName: "last_name",
						FieldName:  "LastName",
						Typ:        reflect.TypeOf(""),
						Offset:     32,
						Ancestors:  []string{"TestModel"},
					},
				},
				ColumnMap: map[string]*ColumnMeta{
					"id": {
						ColumnName:      "id",
						FieldName:       "Id",
						Typ:             reflect.TypeOf(int64(0)),
						IsPrimaryKey:    true,
						IsAutoIncrement: true,
						Ancestors:       []string{"TestModel"},
					},
					"first_name": {
						ColumnName: "first_name",
						FieldName:  "FirstName",
						Typ:        reflect.TypeOf(""),
						Offset:     8,
						Ancestors:  []string{"TestModel"},
					},
					"age": {
						ColumnName: "age",
						FieldName:  "Age",
						Typ:        reflect.TypeOf(int8(0)),
						Offset:     24,
						Ancestors:  []string{"TestModel"},
					},
					"last_name": {
						ColumnName: "last_name",
						FieldName:  "LastName",
						Typ:        reflect.TypeOf(""),
						Offset:     32,
						Ancestors:  []string{"TestModel"},
					},
				},
				Typ: reflect.TypeOf(&TestModel{}),
			},
			input: &TestModel{},
		},
		{
			// 组合
			name: "Combination",
			wantMeta: &TableMeta{
				Columns: []*ColumnMeta{
					{
						ColumnName: "bio",
						FieldName:  "Bio",
						Typ:        reflect.TypeOf(""),
						Offset:     0,
						Ancestors:  []string{"UserModel"},
					},
					{
						ColumnName:      "id",
						FieldName:       "Id",
						Typ:             reflect.TypeOf(int64(0)),
						IsPrimaryKey:    true,
						IsAutoIncrement: true,
						Ancestors:       []string{"TestModel"},
						Offset:          16,
					},
					{
						ColumnName: "first_name",
						FieldName:  "FirstName",
						Typ:        reflect.TypeOf(""),
						Offset:     24,
						Ancestors:  []string{"TestModel"},
					},
					{
						ColumnName: "age",
						FieldName:  "Age",
						Typ:        reflect.TypeOf(int8(0)),
						Offset:     40,
						Ancestors:  []string{"TestModel"},
					},
					{
						ColumnName: "last_name",
						FieldName:  "LastName",
						Typ:        reflect.TypeOf(""),
						Offset:     48,
						Ancestors:  []string{"TestModel"},
					},
				},
				Typ: reflect.TypeOf(&UserModel{}),
				FieldMap: map[string]*ColumnMeta{
					"Bio": {
						ColumnName: "bio",
						FieldName:  "Bio",
						Typ:        reflect.TypeOf(""),
						Offset:     0,
						Ancestors:  []string{"UserModel"},
					},
					"Id": {
						ColumnName:      "id",
						FieldName:       "Id",
						Typ:             reflect.TypeOf(int64(0)),
						IsPrimaryKey:    true,
						IsAutoIncrement: true,
						Ancestors:       []string{"TestModel"},
						Offset:          16,
					},
					"FirstName": {
						ColumnName: "first_name",
						FieldName:  "FirstName",
						Typ:        reflect.TypeOf(""),
						Offset:     24,
						Ancestors:  []string{"TestModel"},
					},
					"Age": {
						ColumnName: "age",
						FieldName:  "Age",
						Typ:        reflect.TypeOf(int8(0)),
						Offset:     40,
						Ancestors:  []string{"TestModel"},
					},
					"LastName": {
						ColumnName: "last_name",
						FieldName:  "LastName",
						Typ:        reflect.TypeOf(""),
						Offset:     48,
						Ancestors:  []string{"TestModel"},
					},
				},
				TableName: "user_model",
				ColumnMap: map[string]*ColumnMeta{
					"bio": {
						ColumnName: "bio",
						FieldName:  "Bio",
						Typ:        reflect.TypeOf(""),
						Offset:     0,
						Ancestors:  []string{"UserModel"},
					},
					"id": {
						ColumnName:      "id",
						FieldName:       "Id",
						Typ:             reflect.TypeOf(int64(0)),
						IsPrimaryKey:    true,
						IsAutoIncrement: true,
						Ancestors:       []string{"TestModel"},
						Offset:          16,
					},
					"first_name": {
						ColumnName: "first_name",
						FieldName:  "FirstName",
						Typ:        reflect.TypeOf(""),
						Offset:     24,
						Ancestors:  []string{"TestModel"},
					},
					"age": {
						ColumnName: "age",
						FieldName:  "Age",
						Typ:        reflect.TypeOf(int8(0)),
						Offset:     40,
						Ancestors:  []string{"TestModel"},
					},
					"last_name": {
						ColumnName: "last_name",
						FieldName:  "LastName",
						Typ:        reflect.TypeOf(""),
						Offset:     48,
						Ancestors:  []string{"TestModel"},
					},
				},
			},
			input: &UserModel{},
		},
		{
			// deep组合
			name: "Deep combination",
			wantMeta: &TableMeta{
				Columns: []*ColumnMeta{
					{
						ColumnName: "email",
						FieldName:  "Email",
						Typ:        reflect.TypeOf(""),
						Offset:     0,
						Ancestors:  []string{"ProfileModel"},
					},
					{
						ColumnName: "password",
						FieldName:  "Password",
						Typ:        reflect.TypeOf(""),
						Offset:     16,
						Ancestors:  []string{"ProfileModel"},
					},
					{
						ColumnName: "bio",
						FieldName:  "Bio",
						Typ:        reflect.TypeOf(""),
						Offset:     32,
						Ancestors:  []string{"UserModel"},
					},
					{
						ColumnName:      "id",
						FieldName:       "Id",
						Typ:             reflect.TypeOf(int64(0)),
						IsPrimaryKey:    true,
						IsAutoIncrement: true,
						Ancestors:       []string{"TestModel"},
						Offset:          48,
					},
					{
						ColumnName: "first_name",
						FieldName:  "FirstName",
						Typ:        reflect.TypeOf(""),
						Offset:     56,
						Ancestors:  []string{"TestModel"},
					},
					{
						ColumnName: "age",
						FieldName:  "Age",
						Typ:        reflect.TypeOf(int8(0)),
						Offset:     72,
						Ancestors:  []string{"TestModel"},
					},
					{
						ColumnName: "last_name",
						FieldName:  "LastName",
						Typ:        reflect.TypeOf(""),
						Offset:     80,
						Ancestors:  []string{"TestModel"},
					},
				},
				Typ: reflect.TypeOf(&ProfileModel{}),
				FieldMap: map[string]*ColumnMeta{
					"Email": {
						ColumnName: "email",
						FieldName:  "Email",
						Typ:        reflect.TypeOf(""),
						Offset:     0,
						Ancestors:  []string{"ProfileModel"},
					},
					"Password": {
						ColumnName: "password",
						FieldName:  "Password",
						Typ:        reflect.TypeOf(""),
						Offset:     16,
						Ancestors:  []string{"ProfileModel"},
					},
					"Bio": {
						ColumnName: "bio",
						FieldName:  "Bio",
						Typ:        reflect.TypeOf(""),
						Offset:     32,
						Ancestors:  []string{"UserModel"},
					},
					"Id": {
						ColumnName:      "id",
						FieldName:       "Id",
						Typ:             reflect.TypeOf(int64(0)),
						IsPrimaryKey:    true,
						IsAutoIncrement: true,
						Ancestors:       []string{"TestModel"},
						Offset:          48,
					},
					"FirstName": {
						ColumnName: "first_name",
						FieldName:  "FirstName",
						Typ:        reflect.TypeOf(""),
						Offset:     56,
						Ancestors:  []string{"TestModel"},
					},
					"Age": {
						ColumnName: "age",
						FieldName:  "Age",
						Typ:        reflect.TypeOf(int8(0)),
						Offset:     72,
						Ancestors:  []string{"TestModel"},
					},
					"LastName": {
						ColumnName: "last_name",
						FieldName:  "LastName",
						Typ:        reflect.TypeOf(""),
						Offset:     80,
						Ancestors:  []string{"TestModel"},
					},
				},
				TableName: "profile_model",
				ColumnMap: map[string]*ColumnMeta{
					"email": {
						ColumnName: "email",
						FieldName:  "Email",
						Typ:        reflect.TypeOf(""),
						Offset:     0,
						Ancestors:  []string{"ProfileModel"},
					},
					"password": {
						ColumnName: "password",
						FieldName:  "Password",
						Typ:        reflect.TypeOf(""),
						Offset:     16,
						Ancestors:  []string{"ProfileModel"},
					},
					"bio": {
						ColumnName: "bio",
						FieldName:  "Bio",
						Typ:        reflect.TypeOf(""),
						Offset:     32,
						Ancestors:  []string{"UserModel"},
					},
					"id": {
						ColumnName:      "id",
						FieldName:       "Id",
						Typ:             reflect.TypeOf(int64(0)),
						IsPrimaryKey:    true,
						IsAutoIncrement: true,
						Ancestors:       []string{"TestModel"},
						Offset:          48,
					},
					"first_name": {
						ColumnName: "first_name",
						FieldName:  "FirstName",
						Typ:        reflect.TypeOf(""),
						Offset:     56,
						Ancestors:  []string{"TestModel"},
					},
					"age": {
						ColumnName: "age",
						FieldName:  "Age",
						Typ:        reflect.TypeOf(int8(0)),
						Offset:     72,
						Ancestors:  []string{"TestModel"},
					},
					"last_name": {
						ColumnName: "last_name",
						FieldName:  "LastName",
						Typ:        reflect.TypeOf(""),
						Offset:     80,
						Ancestors:  []string{"TestModel"},
					},
				},
			},
			input: &ProfileModel{},
		},
		{
			// 忽略组合
			name:  "Ignore combination",
			input: &CustomerModel{},
			wantMeta: &TableMeta{
				Columns: []*ColumnMeta{
					{
						ColumnName:      "id",
						FieldName:       "Id",
						Typ:             reflect.TypeOf(int64(0)),
						IsPrimaryKey:    true,
						IsAutoIncrement: true,
						Ancestors:       []string{"TestModel"},
					},
					{
						ColumnName: "first_name",
						FieldName:  "FirstName",
						Typ:        reflect.TypeOf(""),
						Offset:     8,
						Ancestors:  []string{"TestModel"},
					},
					{
						ColumnName: "age",
						FieldName:  "Age",
						Typ:        reflect.TypeOf(int8(0)),
						Offset:     24,
						Ancestors:  []string{"TestModel"},
					},
					{
						ColumnName: "last_name",
						FieldName:  "LastName",
						Typ:        reflect.TypeOf(""),
						Offset:     32,
						Ancestors:  []string{"TestModel"},
					},
					{
						ColumnName: "company",
						FieldName:  "Company",
						Typ:        reflect.TypeOf(""),
						Offset:     72,
						Ancestors:  []string{"CustomerModel"},
					},
				},
				TableName: "customer_model",
				FieldMap: map[string]*ColumnMeta{
					"Id": {
						ColumnName:      "id",
						FieldName:       "Id",
						Typ:             reflect.TypeOf(int64(0)),
						IsPrimaryKey:    true,
						IsAutoIncrement: true,
						Ancestors:       []string{"TestModel"},
					},
					"FirstName": {
						ColumnName: "first_name",
						FieldName:  "FirstName",
						Typ:        reflect.TypeOf(""),
						Offset:     8,
						Ancestors:  []string{"TestModel"},
					},
					"Age": {
						ColumnName: "age",
						FieldName:  "Age",
						Typ:        reflect.TypeOf(int8(0)),
						Offset:     24,
						Ancestors:  []string{"TestModel"},
					},
					"LastName": {
						ColumnName: "last_name",
						FieldName:  "LastName",
						Typ:        reflect.TypeOf(""),
						Offset:     32,
						Ancestors:  []string{"TestModel"},
					},
					"Company": {
						ColumnName: "company",
						FieldName:  "Company",
						Typ:        reflect.TypeOf(""),
						Offset:     72,
						Ancestors:  []string{"CustomerModel"},
					},
				},
				Typ: reflect.TypeOf(&CustomerModel{}),
				ColumnMap: map[string]*ColumnMeta{
					"id": {
						ColumnName:      "id",
						FieldName:       "Id",
						Typ:             reflect.TypeOf(int64(0)),
						IsPrimaryKey:    true,
						IsAutoIncrement: true,
						Ancestors:       []string{"TestModel"},
					},
					"first_name": {
						ColumnName: "first_name",
						FieldName:  "FirstName",
						Typ:        reflect.TypeOf(""),
						Offset:     8,
						Ancestors:  []string{"TestModel"},
					},
					"age": {
						ColumnName: "age",
						FieldName:  "Age",
						Typ:        reflect.TypeOf(int8(0)),
						Offset:     24,
						Ancestors:  []string{"TestModel"},
					},
					"last_name": {
						ColumnName: "last_name",
						FieldName:  "LastName",
						Typ:        reflect.TypeOf(""),
						Offset:     32,
						Ancestors:  []string{"TestModel"},
					},
					"company": {
						ColumnName: "company",
						FieldName:  "Company",
						Typ:        reflect.TypeOf(""),
						Offset:     72,
						Ancestors:  []string{"CustomerModel"},
					},
				},
			},
		},
		{
			// 冲突
			name:    "Conflict",
			input:   &Conflict{},
			wantErr: errs.NewFieldConflictError("Age"),
		},
		{
			// 指针
			name:    "ptr",
			input:   &TestV2Model{},
			wantErr: errs.ErrDataIsPtr,
		},
		{
			// deep指针
			name:    "deep ptr",
			input:   &TestV3Model{},
			wantErr: errs.ErrDataIsPtr,
		},
		{
			// deep指针2
			name:    "deep ptr2",
			input:   &TestV4Model{},
			wantErr: errs.ErrDataIsPtr,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			registry := &tagMetaRegistry{}
			meta, err := registry.Register(tc.input)
			if err != nil {
				assert.Equal(t, tc.wantErr, err)
				return
			}
			assert.Equal(t, tc.wantMeta, meta)
		})
	}
}

// cpu: Intel(R) Core(TM) i7-6700HQ CPU @ 2.60GHz
// BenchmarkNewMetaRegistry
// BenchmarkNewMetaRegistry-8   	    6144	    182975 ns/op
func BenchmarkNewMetaRegistry(b *testing.B) {
	// 普通
	for i := 0; i < b.N; i++ {
		registry := &tagMetaRegistry{}
		meta, _ := registry.Register(&TestModel{})
		wantMeta := &TableMeta{
			TableName: "test_model",
			Columns: []*ColumnMeta{
				{
					ColumnName:      "id",
					FieldName:       "Id",
					Typ:             reflect.TypeOf(int64(0)),
					IsPrimaryKey:    true,
					IsAutoIncrement: true,
					Ancestors:       []string{"TestModel"},
				},
				{
					ColumnName: "first_name",
					FieldName:  "FirstName",
					Typ:        reflect.TypeOf(""),
					Offset:     8,
					Ancestors:  []string{"TestModel"},
				},
				{
					ColumnName: "age",
					FieldName:  "Age",
					Typ:        reflect.TypeOf(int8(0)),
					Offset:     24,
					Ancestors:  []string{"TestModel"},
				},
				{
					ColumnName: "last_name",
					FieldName:  "LastName",
					Typ:        reflect.TypeOf(""),
					Offset:     32,
					Ancestors:  []string{"TestModel"},
				},
			},
			FieldMap: map[string]*ColumnMeta{
				"Id": {
					ColumnName:      "id",
					FieldName:       "Id",
					Typ:             reflect.TypeOf(int64(0)),
					IsPrimaryKey:    true,
					IsAutoIncrement: true,
					Ancestors:       []string{"TestModel"},
				},
				"FirstName": {
					ColumnName: "first_name",
					FieldName:  "FirstName",
					Typ:        reflect.TypeOf(""),
					Offset:     8,
					Ancestors:  []string{"TestModel"},
				},
				"Age": {
					ColumnName: "age",
					FieldName:  "Age",
					Typ:        reflect.TypeOf(int8(0)),
					Offset:     24,
					Ancestors:  []string{"TestModel"},
				},
				"LastName": {
					ColumnName: "last_name",
					FieldName:  "LastName",
					Typ:        reflect.TypeOf(""),
					Offset:     32,
					Ancestors:  []string{"TestModel"},
				},
			},
			ColumnMap: map[string]*ColumnMeta{
				"id": {
					ColumnName:      "id",
					FieldName:       "Id",
					Typ:             reflect.TypeOf(int64(0)),
					IsPrimaryKey:    true,
					IsAutoIncrement: true,
					Ancestors:       []string{"TestModel"},
				},
				"first_name": {
					ColumnName: "first_name",
					FieldName:  "FirstName",
					Typ:        reflect.TypeOf(""),
					Offset:     8,
					Ancestors:  []string{"TestModel"},
				},
				"age": {
					ColumnName: "age",
					FieldName:  "Age",
					Typ:        reflect.TypeOf(int8(0)),
					Offset:     24,
					Ancestors:  []string{"TestModel"},
				},
				"last_name": {
					ColumnName: "last_name",
					FieldName:  "LastName",
					Typ:        reflect.TypeOf(""),
					Offset:     32,
					Ancestors:  []string{"TestModel"},
				},
			},
			Typ: reflect.TypeOf(&TestModel{}),
		}
		assert.Equal(b, wantMeta, meta)
	}

	// 组合
	for i := 0; i < b.N; i++ {
		registry := &tagMetaRegistry{}
		meta, _ := registry.Register(&UserModel{})
		wantMeta := &TableMeta{
			Columns: []*ColumnMeta{
				{
					ColumnName: "bio",
					FieldName:  "Bio",
					Typ:        reflect.TypeOf(""),
					Offset:     0,
					Ancestors:  []string{"UserModel"},
				},
				{
					ColumnName:      "id",
					FieldName:       "Id",
					Typ:             reflect.TypeOf(int64(0)),
					IsPrimaryKey:    true,
					IsAutoIncrement: true,
					Ancestors:       []string{"TestModel"},
				},
				{
					ColumnName: "first_name",
					FieldName:  "FirstName",
					Typ:        reflect.TypeOf(""),
					Offset:     8,
					Ancestors:  []string{"TestModel"},
				},
				{
					ColumnName: "age",
					FieldName:  "Age",
					Typ:        reflect.TypeOf(int8(0)),
					Offset:     24,
					Ancestors:  []string{"TestModel"},
				},
				{
					ColumnName: "last_name",
					FieldName:  "LastName",
					Typ:        reflect.TypeOf(""),
					Offset:     32,
					Ancestors:  []string{"TestModel"},
				},
			},
			Typ: reflect.TypeOf(&UserModel{}),
			FieldMap: map[string]*ColumnMeta{
				"Bio": {
					ColumnName: "bio",
					FieldName:  "Bio",
					Typ:        reflect.TypeOf(""),
					Offset:     0,
					Ancestors:  []string{"UserModel"},
				},
				"Id": {
					ColumnName:      "id",
					FieldName:       "Id",
					Typ:             reflect.TypeOf(int64(0)),
					IsPrimaryKey:    true,
					IsAutoIncrement: true,
					Ancestors:       []string{"TestModel"},
				},
				"FirstName": {
					ColumnName: "first_name",
					FieldName:  "FirstName",
					Typ:        reflect.TypeOf(""),
					Offset:     8,
					Ancestors:  []string{"TestModel"},
				},
				"Age": {
					ColumnName: "age",
					FieldName:  "Age",
					Typ:        reflect.TypeOf(int8(0)),
					Offset:     24,
					Ancestors:  []string{"TestModel"},
				},
				"LastName": {
					ColumnName: "last_name",
					FieldName:  "LastName",
					Typ:        reflect.TypeOf(""),
					Offset:     32,
					Ancestors:  []string{"TestModel"},
				},
			},
			TableName: "user_model",
			ColumnMap: map[string]*ColumnMeta{
				"bio": {
					ColumnName: "bio",
					FieldName:  "Bio",
					Typ:        reflect.TypeOf(""),
					Offset:     0,
					Ancestors:  []string{"UserModel"},
				},
				"id": {
					ColumnName:      "id",
					FieldName:       "Id",
					Typ:             reflect.TypeOf(int64(0)),
					IsPrimaryKey:    true,
					IsAutoIncrement: true,
					Ancestors:       []string{"TestModel"},
				},
				"first_name": {
					ColumnName: "first_name",
					FieldName:  "FirstName",
					Typ:        reflect.TypeOf(""),
					Offset:     8,
					Ancestors:  []string{"TestModel"},
				},
				"age": {
					ColumnName: "age",
					FieldName:  "Age",
					Typ:        reflect.TypeOf(int8(0)),
					Offset:     24,
					Ancestors:  []string{"TestModel"},
				},
				"last_name": {
					ColumnName: "last_name",
					FieldName:  "LastName",
					Typ:        reflect.TypeOf(""),
					Offset:     32,
					Ancestors:  []string{"TestModel"},
				},
			},
		}
		assert.Equal(b, wantMeta, meta)
	}

	// 嵌套组合
	for i := 0; i < b.N; i++ {
		registry := &tagMetaRegistry{}
		meta, _ := registry.Register(&ProfileModel{})
		wantMeta := &TableMeta{
			Columns: []*ColumnMeta{
				{
					ColumnName: "email",
					FieldName:  "Email",
					Typ:        reflect.TypeOf(""),
					Offset:     0,
					Ancestors:  []string{"ProfileModel"},
				},
				{
					ColumnName: "password",
					FieldName:  "Password",
					Typ:        reflect.TypeOf(""),
					Offset:     16,
					Ancestors:  []string{"ProfileModel"},
				},
				{
					ColumnName: "bio",
					FieldName:  "Bio",
					Typ:        reflect.TypeOf(""),
					Offset:     0,
					Ancestors:  []string{"UserModel"},
				},
				{
					ColumnName:      "id",
					FieldName:       "Id",
					Typ:             reflect.TypeOf(int64(0)),
					IsPrimaryKey:    true,
					IsAutoIncrement: true,
					Ancestors:       []string{"TestModel"},
				},
				{
					ColumnName: "first_name",
					FieldName:  "FirstName",
					Typ:        reflect.TypeOf(""),
					Offset:     8,
					Ancestors:  []string{"TestModel"},
				},
				{
					ColumnName: "age",
					FieldName:  "Age",
					Typ:        reflect.TypeOf(int8(0)),
					Offset:     24,
					Ancestors:  []string{"TestModel"},
				},
				{
					ColumnName: "last_name",
					FieldName:  "LastName",
					Typ:        reflect.TypeOf(""),
					Offset:     32,
					Ancestors:  []string{"TestModel"},
				},
			},
			Typ: reflect.TypeOf(&ProfileModel{}),
			FieldMap: map[string]*ColumnMeta{
				"Email": {
					ColumnName: "email",
					FieldName:  "Email",
					Typ:        reflect.TypeOf(""),
					Offset:     0,
					Ancestors:  []string{"ProfileModel"},
				},
				"Password": {
					ColumnName: "password",
					FieldName:  "Password",
					Typ:        reflect.TypeOf(""),
					Offset:     16,
					Ancestors:  []string{"ProfileModel"},
				},
				"Bio": {
					ColumnName: "bio",
					FieldName:  "Bio",
					Typ:        reflect.TypeOf(""),
					Offset:     0,
					Ancestors:  []string{"UserModel"},
				},
				"Id": {
					ColumnName:      "id",
					FieldName:       "Id",
					Typ:             reflect.TypeOf(int64(0)),
					IsPrimaryKey:    true,
					IsAutoIncrement: true,
					Ancestors:       []string{"TestModel"},
				},
				"FirstName": {
					ColumnName: "first_name",
					FieldName:  "FirstName",
					Typ:        reflect.TypeOf(""),
					Offset:     8,
					Ancestors:  []string{"TestModel"},
				},
				"Age": {
					ColumnName: "age",
					FieldName:  "Age",
					Typ:        reflect.TypeOf(int8(0)),
					Offset:     24,
					Ancestors:  []string{"TestModel"},
				},
				"LastName": {
					ColumnName: "last_name",
					FieldName:  "LastName",
					Typ:        reflect.TypeOf(""),
					Offset:     32,
					Ancestors:  []string{"TestModel"},
				},
			},
			TableName: "profile_model",
			ColumnMap: map[string]*ColumnMeta{
				"email": {
					ColumnName: "email",
					FieldName:  "Email",
					Typ:        reflect.TypeOf(""),
					Offset:     0,
					Ancestors:  []string{"ProfileModel"},
				},
				"password": {
					ColumnName: "password",
					FieldName:  "Password",
					Typ:        reflect.TypeOf(""),
					Offset:     16,
					Ancestors:  []string{"ProfileModel"},
				},
				"bio": {
					ColumnName: "bio",
					FieldName:  "Bio",
					Typ:        reflect.TypeOf(""),
					Offset:     0,
					Ancestors:  []string{"UserModel"},
				},
				"id": {
					ColumnName:      "id",
					FieldName:       "Id",
					Typ:             reflect.TypeOf(int64(0)),
					IsPrimaryKey:    true,
					IsAutoIncrement: true,
					Ancestors:       []string{"TestModel"},
				},
				"first_name": {
					ColumnName: "first_name",
					FieldName:  "FirstName",
					Typ:        reflect.TypeOf(""),
					Offset:     8,
					Ancestors:  []string{"TestModel"},
				},
				"age": {
					ColumnName: "age",
					FieldName:  "Age",
					Typ:        reflect.TypeOf(int8(0)),
					Offset:     24,
					Ancestors:  []string{"TestModel"},
				},
				"last_name": {
					ColumnName: "last_name",
					FieldName:  "LastName",
					Typ:        reflect.TypeOf(""),
					Offset:     32,
					Ancestors:  []string{"TestModel"},
				},
			},
		}
		assert.Equal(b, wantMeta, meta)
	}
}

func TestIgnoreFieldsOption(t *testing.T) {
	tm := &TestIgnoreModel{}
	registry := &tagMetaRegistry{}
	meta, err := registry.Register(tm, IgnoreFieldsOption("Id", "FirstName"))
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, 1, len(meta.Columns))
	assert.Equal(t, 1, len(meta.FieldMap))
	assert.Equal(t, reflect.TypeOf(tm), meta.Typ)
	assert.Equal(t, "test_ignore_model", meta.TableName)

	_, hasId := meta.FieldMap["Id"]
	assert.False(t, hasId)

	_, hasFirstName := meta.FieldMap["FirstName"]
	assert.False(t, hasFirstName)

	_, hasAge := meta.FieldMap["Age"]
	assert.False(t, hasAge)

	_, hasLastName := meta.FieldMap["LastName"]
	assert.True(t, hasLastName)
}

func ExampleMetaRegistry_Get() {
	tm := &TestModel{}
	registry := &tagMetaRegistry{}
	meta, _ := registry.Get(tm)
	fmt.Printf("table name: %v\n", meta.TableName)

	// Output:
	// table name: test_model
}

func ExampleMetaRegistry_Register() {
	// case1 without TableMetaOption
	tm := &TestModel{}
	registry := &tagMetaRegistry{}
	meta, _ := registry.Register(tm)
	fmt.Printf(`
case1：
	table name：%s
	column names：%s,%s,%s,%s
`, meta.TableName, meta.Columns[0].ColumnName, meta.Columns[1].ColumnName, meta.Columns[2].ColumnName, meta.Columns[3].ColumnName)

	// case2 use Tag to ignore field
	tim := &TestIgnoreModel{}
	registry = &tagMetaRegistry{}
	meta, _ = registry.Register(tim)
	fmt.Printf(`
case2：
	table name：%s
	column names：%s,%s
`, meta.TableName, meta.Columns[0].ColumnName, meta.Columns[1].ColumnName)

	// case3 use IgnoreFieldOption to ignore field
	tim = &TestIgnoreModel{}
	registry = &tagMetaRegistry{}
	meta, _ = registry.Register(tim, IgnoreFieldsOption("FirstName"))
	fmt.Printf(`
case3：
	table name：%s
	column names：%s
`, meta.TableName, meta.Columns[0].ColumnName)

	// Output:
	// case1：
	// 	table name：test_model
	// 	column names：id,first_name,age,last_name
	//
	// case2：
	// 	table name：test_ignore_model
	// 	column names：first_name,last_name
	//
	// case3：
	// 	table name：test_ignore_model
	// 	column names：last_name
}

type TestModel struct {
	Id        int64 `eorm:"auto_increment,primary_key"`
	FirstName string
	Age       int8
	LastName  string
}

type UserModel struct {
	Bio string
	TestModel
}

type ProfileModel struct {
	Email    string
	Password string
	UserModel
}

type Conflict struct {
	TestModel
	Age int8
}

type CustomerModel struct {
	TestModel
	InvalidModel `eorm:"-"`
	Company      string
}

type InvalidModel struct {
	*TestModel
	Nickname string
}

type TestV2Model struct {
	*TestModel
}

type TestV3Model struct {
	Mobile string
	InvalidModel
	*TestModel
}

type TestV4Model struct {
	Mobile string
	TestV5Model
}
type TestV5Model struct {
	*TestModel
}

type TestIgnoreModel struct {
	Id        int64 `eorm:"auto_increment,primary_key,-"`
	FirstName string
	Age       int8 `eorm:"-"`
	LastName  string
}
