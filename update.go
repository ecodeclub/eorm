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

type Updater struct {

}

func (u *Updater) Build() (*Query, error) {
	panic("implement me")
}

// Set:
// 1. 更新字段，值从 entity 里面读，也就是从 db.Update(table) 的 table 里面读
// 2. 有特定的指 Set("id", "123")
// 更新多个字段，都是从entity里面读数据，那么我需要 Set(Assign("id", fromEntity), Assign("id", fromEntity))
func (u *Updater) Set(assigns...Assignable) *Updater {
	panic("implement me")
}

func (u *Updater) Where(predicates...Predicate) *Updater {
	panic("implement me")
}
