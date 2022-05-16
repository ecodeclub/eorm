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

package error

import (
	"errors"
	"fmt"
)

var (
	errValueNotSet = errors.New("value unset")
)


// NewInvalidColumnError returns an error represents invalid field name
// TODO(do we need errors pkg?)
func NewInvalidColumnError(field string) error {
	return fmt.Errorf("eorm: invalid column name %s, " +
		"it must be a valid field name of structure", field)
}

func NewValueNotSetError() error {
	return errValueNotSet
}
