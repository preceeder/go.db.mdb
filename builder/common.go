package builder

import (
	"fmt"
	"reflect"
	"regexp"
	"strings"
)

var (
	IgnoreColumnHandlerRe = regexp.MustCompile("^([^.,]+\\.)?(\\d+|'[^']+'|\"[^\"]+\"|`[^`]+`(\\.`[^`]+`.*)?|\\*)$|(\\b[\\w]+\\.\\`[^`]+\\`)")
	DateRe                = regexp.MustCompile("^\\d{4}-\\d{2}-\\d{2}$")
	DateTimeRe            = regexp.MustCompile("^\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}$")
	// sqlStringReplacer 用于批量替换 SQL 字符串中的特殊字符，性能优于多次 ReplaceAll
	sqlStringReplacer = strings.NewReplacer(
		"\\", "\\\\",
		"'", "''",
		":", "::",
	)
)

// ColumnNameHandler 为列名添加反引号，用于防止关键字冲突
// 某些情况不需要处理: 纯数字, 已带引号的字符串, 通配符*
func ColumnNameHandler(field string) string {
	if field == "" {
		return ""
	}
	if IgnoreColumnHandlerRe.MatchString(field) {
		return field
	}
	fields := strings.Split(field, ".")
	lastIndex := len(fields) - 1
	if lastIndex < 0 || fields[lastIndex] == "" {
		return field
	}
	fields[lastIndex] = "`" + fields[lastIndex] + "`"
	return strings.Join(fields, ".")
}

// escapeSQLString 转义 SQL 字符串中的特殊字符
// 注意：这仅用于直接拼接的场景，推荐使用参数化查询
// 使用 Replacer 批量替换，性能优于多次 ReplaceAll
func escapeSQLString(s string) string {
	return sqlStringReplacer.Replace(s)
}

// StringSliceToString 将字符串数组转换为 SQL IN 子句格式的字符串
// 注意：此函数用于直接拼接，存在 SQL 注入风险，推荐使用参数化查询
// []string{"233", "sdf", "er"}  ---> "'233','sdf','er'"
func StringSliceToString(ss []string) string {
	if len(ss) == 0 {
		return ""
	}
	// 预分配容量，减少扩容
	parts := make([]string, 0, len(ss))
	for _, s := range ss {
		parts = append(parts, "'"+escapeSQLString(s)+"'")
	}
	// 使用 strings.Join 代替循环+Truncate，性能更好
	return strings.Join(parts, ",")
}

// NumberSliceToString 将数字数组转换为 SQL IN 子句格式的字符串
// 支持的类型: []int, []int32, []int64, []float32, []float64
func NumberSliceToString(ss any) (string, error) {
	if ss == nil {
		return "", fmt.Errorf("input cannot be nil")
	}

	value := reflect.ValueOf(ss)
	if value.Kind() != reflect.Slice {
		return "", fmt.Errorf("input must be a slice, got %T", ss)
	}

	if value.Len() == 0 {
		return "", nil
	}

	// 预分配切片，使用 strings.Join 提高性能
	parts := make([]string, 0, value.Len())
	for i := 0; i < value.Len(); i++ {
		elem := value.Index(i)
		switch elem.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
			reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
			reflect.Float32, reflect.Float64:
			parts = append(parts, fmt.Sprint(elem.Interface()))
		default:
			return "", fmt.Errorf("unsupported element type in slice: %T, element must be numeric type", elem.Interface())
		}
	}
	return strings.Join(parts, ","), nil
}
