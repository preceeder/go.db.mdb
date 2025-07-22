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
		str = fmt.Sprintf(str, field, key[0])
		value = map[string]any{key[0]: d}
	} else {
		switch d.(type) {
		case Fd:
			dat := d.(Fd)
			str = fmt.Sprintf(str, field, dat.String())
			if dat.Values() != nil {
				value = *dat.Values()
			}
		case string:
			// 这里 : 替换为 ::, 在sqlx里会处理为 :
			str = fmt.Sprintf(str, field, "'"+strings.ReplaceAll(d.(string), ":", "::")+"'")
		case []string:
			d = StringSliceToString(d.([]string))
			str = fmt.Sprintf(str, field, d)

		case []int, []int32, []int64, []float32, []float64:
			d = NumberSliceToString(d)
			str = fmt.Sprintf(str, field, d)

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

// like
// field 列名
// d 比较的数据
// key 占位符的名字
func like(field string, d any, key ...string) Expr {
	str := "%s like %v"
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
			str = "%s in %s"
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
			str = "%s not in %s"
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
func Or(expr ...Expr) Expr {
	bf := bytes.Buffer{}
	bf.WriteString("(")
	//var str = []string{"("}
	var value = map[string]any{}
	for _, an := range expr {
		bf.WriteString("(")
		//str = append(str, "(")
		if ve := an.Values(); ve != nil {
			for k, v := range *ve {
				value[k] = v
			}
		}
		bf.WriteString(an.String())
		bf.WriteString(")")
		bf.WriteString(" or ")
	}
	if bf.Len() > 7 {
		bf.Truncate(bf.Len() - 4)
		bf.WriteString(")")
	} else {
		bf.Truncate(0)
	}

	return Condition{Name: "", Value: &value, S: bf.String()}
}

func And(expr ...Expr) Expr {
	bf := bytes.Buffer{}
	bf.WriteString("(")
	var value = map[string]any{}
	for _, an := range expr {
		bf.WriteString(an.String())
		bf.WriteString(" and ")
		if ve := an.Values(); ve != nil {
			for k, v := range *ve {
				value[k] = v
			}
		}
	}
	if bf.Len() > 6 {
		bf.Truncate(bf.Len() - 5)
		bf.WriteString(")")
	} else {
		bf.Truncate(0)
	}
	return Condition{Name: "", Value: &value, S: bf.String()}
}
func NotExists(str string) Expr {
	bf := bytes.Buffer{}
	bf.WriteString("NOT EXISTS (")
	bf.WriteString(str)
	bf.WriteString(")")
	//str[len(str)-1] = ")"
	return Condition{Name: "", S: bf.String()}
}
