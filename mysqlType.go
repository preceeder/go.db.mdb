package mdb

import (
	"encoding/json"
	"github.com/pkg/errors"
	"strings"
	"time"
)

// 将date  类型转化为 int 类型
type Date int

//	func (d Date) Value() (driver.Value, error) {
//		tempTime, _ := time.Parse("%Y-%m-%d", string(d))
//		return driver.Value(string(tempTime.UnixMilli())), nil
//	}
func (g *Date) Scan(src interface{}) error {
	var source []byte
	// let's support string and []byte
	switch src.(type) {
	case string:
		source = []byte(src.(string))
	case []byte:
		source = src.([]byte)
	case time.Time:
		source = []byte(src.(time.Time).Format(time.DateOnly))
	default:
		return errors.New("Incompatible type for GzippedText")
	}
	v, err := time.Parse(time.DateOnly, string(source))
	if err != nil {
		return err
	}
	*g = Date(v.Second())
	return nil
}

func (g *Date) Date() time.Time {
	return time.Unix(int64(*g), 0)
}

type DateTime time.Time

func (g *DateTime) Scan(src interface{}) error {
	var source []byte
	// let's support string and []byte
	switch src.(type) {
	case string:
		source = []byte(src.(string))
	case []byte:
		source = src.([]byte)
	case time.Time:
		source = []byte(src.(time.Time).String())
	default:
		return errors.New("Incompatible type for GzippedText")
	}
	v, err := time.Parse(time.DateTime, string(source))
	if err != nil {
		return err
	}
	*g = DateTime(v)
	return nil
}

func (g *DateTime) Datetime() time.Time {
	return time.Time(*g)
}

// MarshalJSON 实现：格式化为指定字符串
func (t DateTime) MarshalJSON() ([]byte, error) {
	dattimeStr := t.Datetime().Format("2006-01-02 15:04:05")
	return []byte(`"` + dattimeStr + `"`), nil
}

// UnmarshalJSON 实现：从字符串解析时间
func (t *DateTime) UnmarshalJSON(data []byte) error {
	str := strings.Trim(string(data), `"`)
	parsedTime, err := time.Parse(time.DateTime, str)
	if err != nil {
		return err
	}
	*t = DateTime(parsedTime)
	return nil
}

type Json map[string]any

func (j *Json) Scan(src interface{}) error {
	var source []byte
	switch src.(type) {
	case string:
		source = []byte(src.(string))
	case []byte:
		source = src.([]byte)
	default:
		return errors.New("Incompatible type for string")
	}
	err := json.Unmarshal(source, j)
	if err != nil {
		return errors.New("Incompatible type for string json.Unmarshal error: " + err.Error())
	}
	return nil
}

type JsonSlice []map[string]any

func (j *JsonSlice) Scan(src interface{}) error {
	var source []byte
	switch src.(type) {
	case string:
		source = []byte(src.(string))
	case []byte:
		source = src.([]byte)
	default:
		return errors.New("Incompatible type for string")
	}
	err := json.Unmarshal(source, j)
	if err != nil {
		return errors.New("Incompatible type for string json Unmarshal error: " + err.Error())
	}
	return nil
}

type NullString string

func (i *NullString) Scan(src any) error {
	if src == nil {
		*i = ""
		return nil
	}
	switch src.(type) {
	case time.Time:
		src = []byte(src.(time.Time).String())
	}
	err := convertAssign(i, src)
	return err
}

func (i *NullString) String() string {
	return string(*i)
}

type NullInt int

func (i *NullInt) Scan(src any) error {
	if src == nil {
		*i = 0
		return nil
	}
	err := convertAssign(i, src)
	return err
}

func (i *NullInt) Int() int {
	return int(*i)
}

type NullInt8 int8

func (i *NullInt8) Scan(src any) error {
	if src == nil {
		*i = 0
		return nil
	}
	err := convertAssign(i, src)
	return err
}

func (i *NullInt8) Int8() int8 {
	return int8(*i)
}

type NullInt16 int16

func (i *NullInt16) Scan(src any) error {
	if src == nil {
		*i = 0
		return nil
	}
	err := convertAssign(i, src)
	return err
}
func (i *NullInt16) Int16() int16 {
	return int16(*i)
}

type NullInt32 int32

func (i *NullInt32) Scan(src any) error {
	if src == nil {
		*i = 0
		return nil
	}
	err := convertAssign(i, src)

	return err
}

func (i *NullInt32) Int32() int32 {
	return int32(*i)
}

type NullInt64 int64

func (i *NullInt64) Scan(src any) error {
	if src == nil {
		*i = 0
		return nil
	}
	err := convertAssign(i, src)

	return err
}

func (i *NullInt64) Int64() int64 {
	return int64(*i)
}

func (i *NullInt64) Int() int {
	return int(*i)
}

type NullBool bool

func (i *NullBool) Scan(src any) error {
	if src == nil {
		*i = false
		return nil
	}
	switch src.(type) {
	case int64:
		if src.(int64) != 0 {
			*i = true
			return nil
		} else {
			*i = false
			return nil
		}

	}
	//if src == 1{
	//	*i = true
	//	return nil
	//}else if src == 0{
	//	*i = false
	//	return nil
	//}
	err := convertAssign(i, src)
	return err
}

func (i *NullBool) Bool() bool {
	return bool(*i)
}

type NullByte byte

func (i *NullByte) Scan(src any) error {
	if src == nil {
		*i = 0
		return nil
	}
	err := convertAssign(i, src)
	return err
}

func (i *NullByte) Byte() byte {
	return byte(*i)
}

type NullFloat64 float64

func (i *NullFloat64) Scan(src any) error {
	if src == nil {
		*i = 0
		return nil
	}
	err := convertAssign(i, src)

	return err
}

func (i *NullFloat64) Float64() float64 {

	return float64(*i)
}

type NullFloat32 float32

func (i *NullFloat32) Scan(src any) error {
	if src == nil {
		*i = 0
		return nil
	}
	err := convertAssign(i, src)

	return err
}

func (i *NullFloat32) Float32() float32 {

	return float32(*i)
}
