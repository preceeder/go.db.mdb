package builder

import (
	"bytes"
	"fmt"
	"strings"
)

func handler(str, field string, d any, key ...string) Expr {
	var value map[string]any = make(map[string]any)
	field = ColumnNameHandler(field)
	if len(key) > 0 {
		// 使用参数化查询，更安全
		var bf bytes.Buffer
		bf.WriteString(field)
		bf.WriteString(" = :")
		bf.WriteString(key[0])
		str = bf.String()
		value = map[string]any{key[0]: d}
	} else {
		switch v := d.(type) {
		case Fd:
			// 对于 Fd 类型，直接构建字符串，避免 fmt.Sprintf
			var bf bytes.Buffer
			if strings.HasPrefix(str, "%s") && strings.Contains(str, "%v") {
				// 处理 "%s = %v" 格式
				bf.WriteString(field)
				bf.WriteString(" = ")
				bf.WriteString(v.String())
			} else {
				// 其他格式仍使用 fmt.Sprintf
				str = fmt.Sprintf(str, field, v.String())
				bf.WriteString(str)
			}
			str = bf.String()
			if v.Values() != nil {
				value = *v.Values()
			}
		case string:
			// 注意：直接拼接字符串存在 SQL 注入风险，推荐使用参数化查询（通过 key 参数）
			// 使用 escapeSQLString 转义特殊字符
			escaped := escapeSQLString(v)
			var bf bytes.Buffer
			bf.WriteString(field)
			bf.WriteString(" = '")
			bf.WriteString(escaped)
			bf.WriteString("'")
			str = bf.String()
		case []string:
			// 直接拼接，存在风险
			d = StringSliceToString(v)
			var bf bytes.Buffer
			bf.WriteString(field)
			bf.WriteString(" IN (")
			bf.WriteString(d.(string))
			bf.WriteString(")")
			str = bf.String()
		case []int, []int32, []int64, []float32, []float64:
			// 数字类型相对安全，但仍有风险
			numStr, err := NumberSliceToString(v)
			if err != nil {
				// 如果转换失败，使用原始值（可能导致错误，但不 panic）
				str = fmt.Sprintf(str, field, v)
			} else {
				var bf bytes.Buffer
				bf.WriteString(field)
				bf.WriteString(" IN (")
				bf.WriteString(numStr)
				bf.WriteString(")")
				str = bf.String()
			}
		default:
			str = fmt.Sprintf(str, field, d)
		}
	}
	return Condition{Name: field, Value: &value, S: str}
}

// Eq
// - field 列名
// - d 比较的数据
// - key 占位符的名字
func eq(field string, d any, key ...string) Expr {
	var str string = "%s = %v"
	if len(key) > 0 {
		str = "%s = :%s"
	}
	return handler(str, field, d, key...)
}

// Eq
// - field 列名
// - d 比较的数据
// - key 占位符的名字
func notEq(field string, d any, key ...string) Expr {
	var str string = "%s != %v"
	if len(key) > 0 {
		str = "%s != :%s"
	}
	return handler(str, field, d, key...)
}

// Neq
// field 列名
// d 比较的数据
// key 占位符的名字
func neq(field string, d any, key ...string) Expr {
	var str string = "%s <> %v"
	if len(key) > 0 {
		str = "%s <> :%s"
	}
	return handler(str, field, d, key...)
}

// Gt
// field 列名
// d 比较的数据
// key 占位符的名字
func gt(field string, d any, key ...string) Expr {
	str := "%s > %v"
	if len(key) > 0 {
		str = "%s > :%s"
	}
	return handler(str, field, d, key...)
}

// Gte
// field 列名
// d 比较的数据
// key 占位符的名字
func gte(field string, d any, key ...string) Expr {
	str := "%s >= %v"
	if len(key) > 0 {
		str = "%s >= :%s"
	}
	return handler(str, field, d, key...)
}

// Lt
// field 列名
// d 比较的数据
// key 占位符的名字
func lt(field string, d any, key ...string) Expr {
	str := "%s < %v"
	if len(key) > 0 {
		str = "%s < :%s"
	}
	return handler(str, field, d, key...)
}

// like 模糊查询
// field 列名
// d 比较的数据（字符串）
// key 占位符的名字（推荐使用，避免 SQL 注入）
func like(field string, d any, key ...string) Expr {
	var str string
	if len(key) > 0 {
		str = "%s LIKE :%s"
	} else {
		str = "%s LIKE %v"
	}
	return handler(str, field, d, key...)
}

// Lte
// field 列名
// d 比较的数据
// key 占位符的名字
func lte(field string, d any, key ...string) Expr {
	str := "%s <= %v"
	if len(key) > 0 {
		str = "%s <= :%s"
	}
	return handler(str, field, d, key...)
}

// IsNotNull
// field 列名
func isNotNull(field string) Expr {
	str := fmt.Sprintf("%s is not null", field)
	return Condition{Name: field, S: str}
}

// name Symbol value
// func(value)
// name symbol
// IsNull
// field 列名
func isNull(field string) Expr {
	str := fmt.Sprintf("%s is null", field)
	return Condition{Name: field, S: str}
}

// In
// field 列名
// d 比较的数据
// key 占位符的名字
func in(field string, d any, key ...string) Expr {
	var str string
	if len(key) > 0 {
		str = "%s in (:%s)"
	} else {
		switch d.(type) {
		// 说明后一个 d 是sql查询
		case string:
			// 子查询
            str = "%s in (%s)"
			field = ColumnNameHandler(field)
			var value map[string]any = make(map[string]any)
			str = fmt.Sprintf(str, field, d)
			return Condition{Name: field, Value: &value, S: str}
		default:
			str = "%s in (%s)"
		}
	}
	return handler(str, field, d, key...)
}

// NotIn
// field 列名
// d 比较的数据
// key 占位符的名字
func notIn(field string, d any, key ...string) Expr {
	var str string
	if len(key) > 0 {
		str = "%s not in (:%s)"
	} else {
		switch d.(type) {
		// 说明后一个 d 是sql查询
		case string:
			// 子查询
            str = "%s not in (%s)"
			field = ColumnNameHandler(field)
			var value map[string]any = make(map[string]any)
			str = fmt.Sprintf(str, field, d)
			return Condition{Name: field, Value: &value, S: str}
		default:
			str = "%s not in (%s)"
		}
	}
	return handler(str, field, d, key...)
}

func NotExists(str string) Expr {
	bf := bytes.Buffer{}
	bf.WriteString("NOT EXISTS (")
	bf.WriteString(str)
	bf.WriteString(")")
	//str[len(str)-1] = ")"
	return Condition{Name: "", S: bf.String()}
}

//	[][]Expr{
//		{
//			Eq("x1", 11),
//			Gte("x2", 45),
//		},
//		{
//			Eq("x3", "234"),
//			Neq("x4", "tx2"),
//		},
//	},
// Or 组合多个表达式，使用 OR 连接
// 示例: Or(Eq("x1", 11), Gte("x2", 45)) -> ((x1 = 11) OR (x2 >= 45))
func Or(expr ...Expr) Expr {
	if len(expr) == 0 {
		return Condition{Name: "", Value: &map[string]any{}, S: ""}
	}

	bf := bytes.Buffer{}
	bf.WriteString("(")
	var value = map[string]any{}
	for _, an := range expr {
		bf.WriteString("(")
		if ve := an.Values(); ve != nil {
			for k, v := range *ve {
				value[k] = v
			}
		}
		bf.WriteString(an.String())
		bf.WriteString(") OR ")
	}
	// 移除最后一个 " OR "
	if bf.Len() > 5 {
		bf.Truncate(bf.Len() - 5)
	}
	bf.WriteString(")")

	return Condition{Name: "", Value: &value, S: bf.String()}
}

// And 组合多个表达式，使用 AND 连接
// 示例: And(Eq("x1", 11), Gte("x2", 45)) -> ((x1 = 11) AND (x2 >= 45))
func And(expr ...Expr) Expr {
	if len(expr) == 0 {
		return Condition{Name: "", Value: &map[string]any{}, S: ""}
	}

	bf := bytes.Buffer{}
	bf.WriteString("(")
	var value = map[string]any{}
	for _, an := range expr {
		if ve := an.Values(); ve != nil {
			for k, v := range *ve {
				value[k] = v
			}
		}
		bf.WriteString(an.String())
		bf.WriteString(" AND ")
	}
	// 移除最后一个 " AND "
	if bf.Len() > 6 {
		bf.Truncate(bf.Len() - 5)
	}
	bf.WriteString(")")

	return Condition{Name: "", Value: &value, S: bf.String()}
}
