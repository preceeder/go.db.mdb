package builder

import (
	"bytes"
	"fmt"
	"reflect"
	"regexp"
	"strings"
)

var IgnoreColumnHandlerRe = regexp.MustCompile("^([^.,]+.|)(\\d+|'[\\S\\s]+'|\"[\\S\\s]+\"|`[\\S\\s]+`(\\.`[^`]+`.*|)|\\*)$")
var DateRe = regexp.MustCompile("^\\d{4}-\\d{2}-\\d{2}$")
var DateTimeRe = regexp.MustCompile("^\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}\\d{2}$")

// ColumnNameHandler 列名添加 “
// 有些是不需要的: 纯数字, 单引号,双引号的
func ColumnNameHandler(field string) string {
	if IgnoreColumnHandlerRe.MatchString(field) {
		return field
	}
	fields := strings.Split(field, ".")
	lastIndex := len(fields) - 1
	if fields[lastIndex] == "" {
		return ""
	}
	fields[lastIndex] = "`" + fields[lastIndex] + "`"
	return strings.Join(fields, ".")
}

// StringSliceToString 将 字符串数组 转为 每个对象有单引号的 字符串
// []string{"233", "sdf", "er"}  ---> "'233','sdf','er'"
func StringSliceToString(ss []string) string {
	bf := bytes.Buffer{} //存放序列化结果
	for _, s := range ss {
		bf.WriteString("'")
		bf.WriteString(strings.ReplaceAll(s, ":", "::"))
		bf.WriteString("'")
		bf.WriteString(",")
	}
	// 去调最后一个 ,
	bf.Truncate(bf.Len() - 1)
	return bf.String()
}

// StringSliceToString 将 字符串数组 转为 每个对象有单引号的 字符串
// []string{"233", "sdf", "er"}  ---> "'233','sdf','er'"
func NumberSliceToString(ss any) string {

	value := reflect.ValueOf(ss)
	switch value.Kind() {
	case reflect.Slice:
		bf := bytes.Buffer{} //存放序列化结果
		for i := 0; i < value.Len(); i++ {
			bf.WriteString(fmt.Sprint(value.Index(i).Interface()))
			bf.WriteString(",")
		}
		// 去调最后一个 ,
		bf.Truncate(bf.Len() - 1)
		return bf.String()
	default:
		panic("value not support, must []int | []int32 | []int64 | []float32 | []float64")
	}

}
