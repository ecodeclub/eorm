// Copyright 2021 ecodeclub
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

package utils

import (
	"database/sql"
	_ "database/sql"
	_ "unsafe"

	"github.com/ecodeclub/eorm/internal/merger/internal/errs"
)

//go:linkname ConvertAssign database/sql.convertAssign
func ConvertAssign(dest, src any) error

func ConvertNullable(dest, src any) error {
	switch src.(type) {
	case sql.NullInt64:
		newSrc := src.(sql.NullInt64)
		newDest, ok := dest.(*sql.NullInt64)
		if !ok {
			return errs.ErrMergerNullAble
		}
		if !newSrc.Valid {
			newDest.Valid = false
			return nil
		}
		newDest.Valid = true
		newDest.Int64 = newSrc.Int64

	case sql.NullFloat64:
		newSrc := src.(sql.NullFloat64)
		newDest, ok := dest.(*sql.NullFloat64)
		if !ok {
			return errs.ErrMergerNullAble
		}
		if !newSrc.Valid {
			newDest.Valid = false
			return nil
		}
		newDest.Valid = true
		newDest.Float64 = newSrc.Float64
	case sql.NullString:
		newSrc := src.(sql.NullString)
		newDest, ok := dest.(*sql.NullString)
		if !ok {
			return errs.ErrMergerNullAble
		}
		if !newSrc.Valid {
			newDest.Valid = false
			return nil
		}
		newDest.Valid = true
		newDest.String = newSrc.String
	case sql.NullTime:
		newSrc := src.(sql.NullTime)
		newDest, ok := dest.(*sql.NullTime)
		if !ok {
			return errs.ErrMergerNullAble
		}
		if !newSrc.Valid {
			newDest.Valid = false
			return nil
		}
		newDest.Valid = true
		newDest.Time = newSrc.Time
	case sql.NullByte:
		newSrc := src.(sql.NullByte)
		newDest, ok := dest.(*sql.NullByte)
		if !ok {
			return errs.ErrMergerNullAble
		}
		if !newSrc.Valid {
			newDest.Valid = false
			return nil
		}
		newDest.Valid = true
		newDest.Byte = newSrc.Byte
	case sql.NullBool:
		newSrc := src.(sql.NullBool)
		newDest, ok := dest.(*sql.NullBool)
		if !ok {
			return errs.ErrMergerNullAble
		}
		if !newSrc.Valid {
			newDest.Valid = false
			return nil
		}
		newDest.Valid = true
		newDest.Bool = newSrc.Bool
	case sql.NullInt16:
		newSrc := src.(sql.NullInt16)
		newDest, ok := dest.(*sql.NullInt16)
		if !ok {
			return errs.ErrMergerNullAble
		}
		if !newSrc.Valid {
			newDest.Valid = false
			return nil
		}
		newDest.Valid = true
		newDest.Int16 = newSrc.Int16
	case sql.NullInt32:
		newSrc := src.(sql.NullInt32)
		newDest, ok := dest.(*sql.NullInt32)
		if !ok {
			return errs.ErrMergerNullAble
		}
		if !newSrc.Valid {
			newDest.Valid = false
			return nil
		}
		newDest.Valid = true
		newDest.Int32 = newSrc.Int32
	default:
		return ConvertAssign(dest, src)
	}
	return nil
}
