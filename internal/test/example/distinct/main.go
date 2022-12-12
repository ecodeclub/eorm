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

package main

import (
	"context"
	"fmt"
	"log"

	_ "github.com/go-sql-driver/mysql"
	"github.com/gotomicro/eorm"
	"github.com/gotomicro/eorm/internal/test"
)

func main() {
	// 准备数据，插入3行数据
	db, err := eorm.Open("mysql", "root:root@tcp(localhost:13306)/integration_test")
	if err != nil {
		log.Fatal(err)
	}
	col1 := test.NewSimpleStruct(1)
	col2 := test.NewSimpleStruct(2)
	col3 := test.NewSimpleStruct(3)
	res := eorm.NewInserter[test.SimpleStruct](db).Values(col1, col2, col3).Exec(context.Background())
	if res.Err() != nil {
		log.Fatal(err)
	}
	// 第一种 distinct某个列的形式：SELECT DISTINCT xxx FROM xxx。直接调用Distinct方法即可
	ans, err := eorm.NewSelector[test.SimpleStruct](db).Select(eorm.C("Int")).Distinct().GetMulti(context.Background())
	if err != nil {
		log.Fatal(err)
	}
	for _, a := range ans {
		fmt.Println(a.Int)
	}

	// 第二种 在聚合函数中使用distinct的形式：SELECT COUNT(DISTINCT xxx) FROM xxx; 需要在Select中传入CountDistinct，AvgDistinct, SumDistinct方法
	ans2, err := eorm.NewSelector[int](db).Select(eorm.CountDistinct("Bool")).From(&test.SimpleStruct{}).GetMulti(context.Background())
	if err != nil {
		log.Fatal(err)
	}

	for _, a := range ans2 {
		fmt.Println(*a)
	}

	// 第三种 在having的聚合函数中使用distinct的形式： SELECT xxx FROM xxx GROUP BY xxx HAVING COUNT(DISTINCT xxx)=?,使用形式Having方法中传入CountDistinct，AvgDistinct, SumDistinct方法。
	ans3, err := eorm.NewSelector[test.SimpleStruct](db).From(&test.SimpleStruct{}).Select(eorm.C("JsonColumn")).GroupBy("JsonColumn").Having(eorm.CountDistinct("JsonColumn").EQ(1)).GetMulti(context.Background())
	if err != nil {
		log.Fatal(err)
	}
	for _, a := range ans3 {
		fmt.Println(a.JsonColumn)
	}

	// 清理数据
	res = eorm.RawQuery[any](db, "DELETE FROM `simple_struct`").Exec(context.Background())
	if res.Err() != nil {
		log.Fatal(res.Err())
	}

}
