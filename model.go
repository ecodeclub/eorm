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

package eql

import (
	"reflect"
)

// TableMeta represents data model, or a table
type TableMeta struct {
	tableName string
	columns[] *ColumnMeta
	// key 是字段名
	fieldMap map[string]*ColumnMeta
	typ reflect.Type
}

// ColumnMeta represents model's field, or column
type ColumnMeta struct {
	columnName string
	fieldName string
	typ reflect.Type
	isPrimaryKey bool
	isAutoIncrement bool
}

type tableMetaOption func(meta *TableMeta)

type MetaRegistry interface {
	// 这里传过来的 table 应该是结构体指针，例如 *User
	Get(table interface{}) (*TableMeta, error)
	Register(table interface{}, opts...tableMetaOption) (*TableMeta, error)
}

var defaultMetaRegistry = &tagMetaRegistry{}

// 我们的默认实现，它使用如下的规则
// 1. 结构体名字转表名，按照驼峰转下划线的方式
// 2. 字段名转列名，也是按照驼峰转下划线的方式
type tagMetaRegistry struct {
	metas map[reflect.Type]*TableMeta
}

func (t *tagMetaRegistry) Get(table interface{}) (*TableMeta, error) {
	// 从 metas 里面去取，没有的话就调用 Register 新注册一个
	panic("implement me")
}

func (t *tagMetaRegistry) Register(table interface{}, opts ...tableMetaOption) (*TableMeta, error) {
	// 拿到 table 的反射
	// 遍历 table 的所有字段
	// 对每一个字段检查 tag eql, 如果 eql 里面有 auto_increment, primary_key，就相应设置 ColumnMeta的值
	// 最后处理所有的 opts
	// 塞到 map 里面
	panic("implement me")
}

