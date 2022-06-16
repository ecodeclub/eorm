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

package eorm

import "fmt"

func ExampleAssign() {
	db := memoryDB()
	tm := &TestModel{}
	examples := []struct {
		assign    Assignment
		assignStr string
		wantSQL   string
		wantArgs  []interface{}
	}{
		{
			assign:    Assign("Age", 18),
			assignStr: `Assign("Age", 18)`,
			wantSQL:   "UPDATE `test_model` SET `age`=?;",
			wantArgs:  []interface{}{18},
		},
		{
			assign:    Assign("Age", C("Id")),
			assignStr: `Assign("Age", C("Id"))`,
			wantSQL:   "UPDATE `test_model` SET `age`=`id`;",
		},
		{
			assign:    Assign("Age", C("Age").Add(1)),
			assignStr: `Assign("Age", C("Age").Add(1))`,
			wantSQL:   "UPDATE `test_model` SET `age`=`age`+?;",
			wantArgs:  []interface{}{1},
		},
		{
			assign:    Assign("Age", Raw("`age`+`id`+1")),
			assignStr: "Assign(\"Age\", Raw(\"`age`+`id`+1\"))",
			wantSQL:   "UPDATE `test_model` SET `age`=`age`+`id`+1;",
		},
	}
	for _, exp := range examples {
		query, _ := db.Update(tm).Set(exp.assign).Build()
		fmt.Printf(`
Assignment: %s
SQL: %s
Args: %v
`, exp.assignStr, query.SQL, query.Args)
	}
	// Output:
	//
	// Assignment: Assign("Age", 18)
	// SQL: UPDATE `test_model` SET `age`=?;
	// Args: [18]
	//
	// Assignment: Assign("Age", C("Id"))
	// SQL: UPDATE `test_model` SET `age`=`id`;
	// Args: []
	//
	// Assignment: Assign("Age", C("Age").Add(1))
	// SQL: UPDATE `test_model` SET `age`=(`age`+?);
	// Args: [1]
	//
	// Assignment: Assign("Age", Raw("`age`+`id`+1"))
	// SQL: UPDATE `test_model` SET `age`=`age`+`id`+1;
	// Args: []
}
