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

type config struct {
	dialect Dialect
}

var (
	defaultConfig *config
)

// Option represents EQL option.
// Usually it means global config
type Option func(cfg *config)

// Init the EQL
func Init(opts ...Option) {
	defaultConfig = &config{
		dialect: mysql,
	}
}

// WithDefaultDialect init the default dialect. Or SQL builder will use this dialect if not specified
func WithDefaultDialect(dialect Dialect) Option {
	return func(cfg *config) {
		cfg = defaultConfig
	}
}
