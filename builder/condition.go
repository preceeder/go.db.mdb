package builder

import (
	"bytes"
	"fmt"
)

func handler(str, field string, d any, key ...string) Expr {
	field = ColumnNameHandler(field)
	value := make(map[string]any)

	if len(key) > 0 {
		value[key[0]] = d
		str = fmt.Sprintf(str, field, key[0])
		return Condition{Name: field, Value: &value, S: str}
	}

	switch v := d.(type) {
	case Fd:
		str = fmt.Sprintf(str, field, v.String())
		if ve := v.Values(); ve != nil {
			for k, val := range *ve {
				value[k] = val
			}
		}
	case Expr:
		str = fmt.Sprintf(str, field, v.String())
		if ve := v.Values(); ve != nil {
			for k, val := range *ve {
				value[k] = val
			}
		}
	case Field:
		str = fmt.Sprintf(str, field, v.String())
		if ve := v.Values(); ve != nil {
			for k, val := range *ve {
				value[k] = val
			}
		}
	case string:
		escaped := escapeSQLString(v)
		str = fmt.Sprintf(str, field, "'"+escaped+"'")
	case []string:
		str = fmt.Sprintf(str, field, StringSliceToString(v))
	case []int, []int32, []int64, []float32, []float64:
		if numStr, err := NumberSliceToString(v); err == nil {
			str = fmt.Sprintf(str, field, numStr)
		} else {
			str = fmt.Sprintf(str, field, v)
		}
	default:
		str = fmt.Sprintf(str, field, d)
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
//
// Or 组合多个表达式，使用 OR 连接
// 示例: Or(Eq("x1", 11), Gte("x2", 45)) -> (x1 = 11 OR x2 >= 45)
func Or(expr ...Expr) Expr {
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
		bf.WriteString(" OR ")
	}
	// 移除最后一个 " OR "
	if bf.Len() > 4 {
		bf.Truncate(bf.Len() - 4)
	}
	bf.WriteString(")")

	return Condition{Name: "", Value: &value, S: bf.String()}
}

// And 组合多个表达式，使用 AND 连接
// 示例: And(Eq("x1", 11), Gte("x2", 45)) -> (x1 = 11 AND x2 >= 45)
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
