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
	"sync"
)

// TableMeta represents data model, or a table
type TableMeta struct {
	tableName string
	columns   []ColumnMeta
}

// ColumnMeta represents model's field, or column
type ColumnMeta struct {
	columnName      string
	isPrimaryKey    bool
	isAutoIncrement bool
	notNull         bool
	re              reflect.Type
}

type MetaTableFactory interface {
	Get(tableName interface{}) (*ColumnMeta, error)
	Set(tableName interface{}, column *ColumnMeta) (*ColumnMeta, error)
}

type NewMetaTable struct {
	syMap   sync.Map
	syMutex sync.Mutex
}

func (meta *NewMetaTable) Get(tableName interface{}) (*ColumnMeta, error) {
	if v, ok := meta.syMap.Load(tableName); ok {
		return v.(*ColumnMeta), nil
	}
	return nil, nil
}

func (meta *NewMetaTable) Set(tableName interface{}, column *ColumnMeta) (*ColumnMeta, error) {
	metaData, err := meta.Get(tableName)
	if err != nil {
		return nil, err
	}
	if metaData != nil {
		return metaData, nil
	}
	meta.syMutex.Lock()
	defer meta.syMutex.Unlock()
	meta.syMap.Store(tableName, column)
	return column, nil
}
