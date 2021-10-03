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
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestStrictNullableFunc(t *testing.T) {
	str := "Hello"
	assert.False(t, NilAsNullFunc(str))
	str = ""
	assert.False(t, NilAsNullFunc(str))
	var err error
	assert.True(t, NilAsNullFunc(err))

	var i int
	assert.False(t, NilAsNullFunc(i))
}

func TestZeroAsNullableFunc(t *testing.T) {
	assert.True(t, ZeroAsNullFunc(0))
	assert.True(t, ZeroAsNullFunc(int8(0)))
	assert.True(t, ZeroAsNullFunc(int16(0)))
	assert.True(t, ZeroAsNullFunc(int32(0)))
	assert.True(t, ZeroAsNullFunc(int64(0)))
	assert.True(t, ZeroAsNullFunc(uint(0)))
	assert.True(t, ZeroAsNullFunc(uint8(0)))
	assert.True(t, ZeroAsNullFunc(uint16(0)))
	assert.True(t, ZeroAsNullFunc(uint32(0)))
	assert.True(t, ZeroAsNullFunc(uint64(0)))
	assert.True(t, ZeroAsNullFunc(float32(0)))
	assert.True(t, ZeroAsNullFunc(float64(0)))
	assert.True(t, ZeroAsNullFunc(""))
	var err error
	assert.True(t, ZeroAsNullFunc(err))
}

