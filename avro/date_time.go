// Code generated by github.com/actgardner/gogen-avro/v7. DO NOT EDIT.
/*
 * SOURCE:
 *     Record.avsc
 */
package avro

import (
	"github.com/actgardner/gogen-avro/v7/compiler"
	"github.com/actgardner/gogen-avro/v7/vm"
	"github.com/actgardner/gogen-avro/v7/vm/types"
	"io"
)

type DateTime struct {
	Year *UnionNullInt `json:"year"`

	Month *UnionNullInt `json:"month"`

	Day *UnionNullInt `json:"day"`

	Hour *UnionNullInt `json:"hour"`

	Minute *UnionNullInt `json:"minute"`

	Second *UnionNullInt `json:"second"`

	Millis *UnionNullInt `json:"millis"`
}

const DateTimeAvroCRC64Fingerprint = "\xef!̾罐\xf1"

func NewDateTime() *DateTime {
	return &DateTime{}
}

func DeserializeDateTime(r io.Reader) (*DateTime, error) {
	t := NewDateTime()
	deser, err := compiler.CompileSchemaBytes([]byte(t.Schema()), []byte(t.Schema()))
	if err != nil {
		return nil, err
	}

	err = vm.Eval(r, deser, t)
	if err != nil {
		return nil, err
	}
	return t, err
}

func DeserializeDateTimeFromSchema(r io.Reader, schema string) (*DateTime, error) {
	t := NewDateTime()

	deser, err := compiler.CompileSchemaBytes([]byte(schema), []byte(t.Schema()))
	if err != nil {
		return nil, err
	}

	err = vm.Eval(r, deser, t)
	if err != nil {
		return nil, err
	}
	return t, err
}

func writeDateTime(r *DateTime, w io.Writer) error {
	var err error
	err = writeUnionNullInt(r.Year, w)
	if err != nil {
		return err
	}
	err = writeUnionNullInt(r.Month, w)
	if err != nil {
		return err
	}
	err = writeUnionNullInt(r.Day, w)
	if err != nil {
		return err
	}
	err = writeUnionNullInt(r.Hour, w)
	if err != nil {
		return err
	}
	err = writeUnionNullInt(r.Minute, w)
	if err != nil {
		return err
	}
	err = writeUnionNullInt(r.Second, w)
	if err != nil {
		return err
	}
	err = writeUnionNullInt(r.Millis, w)
	if err != nil {
		return err
	}
	return err
}

func (r *DateTime) Serialize(w io.Writer) error {
	return writeDateTime(r, w)
}

func (r *DateTime) Schema() string {
	return "{\"fields\":[{\"default\":null,\"name\":\"year\",\"type\":[\"null\",\"int\"]},{\"default\":null,\"name\":\"month\",\"type\":[\"null\",\"int\"]},{\"default\":null,\"name\":\"day\",\"type\":[\"null\",\"int\"]},{\"default\":null,\"name\":\"hour\",\"type\":[\"null\",\"int\"]},{\"default\":null,\"name\":\"minute\",\"type\":[\"null\",\"int\"]},{\"default\":null,\"name\":\"second\",\"type\":[\"null\",\"int\"]},{\"default\":null,\"name\":\"millis\",\"type\":[\"null\",\"int\"]}],\"name\":\"com.alibaba.dts.formats.avro.DateTime\",\"type\":\"record\"}"
}

func (r *DateTime) SchemaName() string {
	return "com.alibaba.dts.formats.avro.DateTime"
}

func (_ *DateTime) SetBoolean(v bool)    { panic("Unsupported operation") }
func (_ *DateTime) SetInt(v int32)       { panic("Unsupported operation") }
func (_ *DateTime) SetLong(v int64)      { panic("Unsupported operation") }
func (_ *DateTime) SetFloat(v float32)   { panic("Unsupported operation") }
func (_ *DateTime) SetDouble(v float64)  { panic("Unsupported operation") }
func (_ *DateTime) SetBytes(v []byte)    { panic("Unsupported operation") }
func (_ *DateTime) SetString(v string)   { panic("Unsupported operation") }
func (_ *DateTime) SetUnionElem(v int64) { panic("Unsupported operation") }

func (r *DateTime) Get(i int) types.Field {
	switch i {
	case 0:
		r.Year = NewUnionNullInt()

		return r.Year
	case 1:
		r.Month = NewUnionNullInt()

		return r.Month
	case 2:
		r.Day = NewUnionNullInt()

		return r.Day
	case 3:
		r.Hour = NewUnionNullInt()

		return r.Hour
	case 4:
		r.Minute = NewUnionNullInt()

		return r.Minute
	case 5:
		r.Second = NewUnionNullInt()

		return r.Second
	case 6:
		r.Millis = NewUnionNullInt()

		return r.Millis
	}
	panic("Unknown field index")
}

func (r *DateTime) SetDefault(i int) {
	switch i {
	case 0:
		r.Year = nil
		return
	case 1:
		r.Month = nil
		return
	case 2:
		r.Day = nil
		return
	case 3:
		r.Hour = nil
		return
	case 4:
		r.Minute = nil
		return
	case 5:
		r.Second = nil
		return
	case 6:
		r.Millis = nil
		return
	}
	panic("Unknown field index")
}

func (r *DateTime) NullField(i int) {
	switch i {
	case 0:
		r.Year = nil
		return
	case 1:
		r.Month = nil
		return
	case 2:
		r.Day = nil
		return
	case 3:
		r.Hour = nil
		return
	case 4:
		r.Minute = nil
		return
	case 5:
		r.Second = nil
		return
	case 6:
		r.Millis = nil
		return
	}
	panic("Not a nullable field index")
}

func (_ *DateTime) AppendMap(key string) types.Field { panic("Unsupported operation") }
func (_ *DateTime) AppendArray() types.Field         { panic("Unsupported operation") }
func (_ *DateTime) Finalize()                        {}

func (_ *DateTime) AvroCRC64Fingerprint() []byte {
	return []byte(DateTimeAvroCRC64Fingerprint)
}
