package db

import (
	"encoding/json"
	"github.com/pkg/errors"
	"strings"
	"time"
)

// 将date  类型转化为 int 类型
type Date int

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
