package builder

import (
	"bytes"
	"fmt"
)

// If 判断
func If(expr any, v1, v2 any) Fd {
	f := &Fd{
		v: []any{expr, v1, v2},
		s: "IF(%s, %v, %v)",
	}
	f.LabelHandler()
	return *f
}

// IfNull 判断
func IfNull(expr any, v1 any) Fd {
	f := &Fd{
		v: []any{expr, v1},
		s: "IFNULL(%s, %v)",
	}
	f.LabelHandler()
	return *f
}

// Sum
// field 字段名
// label 别名
func Sum(field any) Fd {
	f := &Fd{
		v: []any{field},
		s: "SUM(%v)",
	}
	f.LabelHandler()
	return *f
}

func Count(field any) Fd {
	f := &Fd{
		v: []any{field},
		s: "COUNT(%v)",
	}
	f.LabelHandler()
	return *f
}

func Min(field string) Fd {
	f := &Fd{
		field: field,
		v:     []any{field},
		s:     "MIN(%s)",
	}
	f.ColumnNameHandler()
	f.LabelHandler()
	return *f
}

func Max(field string) Fd {
	f := &Fd{
		field: field,
		v:     []any{field},
		s:     "MAX(%s)",
	}
	f.ColumnNameHandler()
	f.LabelHandler()
	return *f
}

// AddDate 计算起始日期 d 加上 n 天的日期
// SELECT ADDDATE("2017-06-15", INTERVAL 10 DAY); ->2017-06-25
func AddDate(startDate string, n string) Fd {
	f := &Fd{
		s: "ADDDATE(%s, INTERVAL %d DAY)",
		v: make([]any, 0),
	}
	if DateRe.MatchString(startDate) {
		f.v = []any{"'" + startDate + "'"}
	} else {
		f.field = startDate
		f.v = append(f.v, f.field)
	}
	f.v = append(f.v, n)

	f.ColumnNameHandler()
	f.LabelHandler()
	return *f
}

func DateFormat(fd Fd, format string) Fd {
	str := "DATE_FORMAT(%v, %s)"
	f := &Fd{
		v: []any{fd, format},
		s: str,
	}
	f.ColumnNameHandler()

	f.LabelHandler()
	return *f
}

// AddTime n 是一个时间表达式，时间 t 加上时间表达式 n
// 加 5 秒： SELECT ADDTIME('2011-11-11 11:11:11', 5); ->2011-11-11 11:11:16 (秒)
// 添加 2 小时, 10 分钟, 5 秒: SELECT ADDTIME("2020-06-15 09:34:21", "2:10:5"); -> 2020-06-15 11:44:26
func AddTime(startTime string, atime any) Fd {
	f := &Fd{
		s: "ADDTIME(%s, %v)",
	}
	if DateTimeRe.MatchString(startTime) {
		f.v = []any{"'" + startTime + "'"}
	} else {
		f.field = startTime
		f.v = append(f.v, f.field)

	}
	f.v = append(f.v, atime)

	f.ColumnNameHandler()
	f.LabelHandler()
	return *f
}

// CurDate 返回当前日期
func CurDate() Fd {
	f := &Fd{
		s: "CURDATE()",
		v: make([]any, 0),
	}
	f.LabelHandler()
	return *f
}

func ConcatGroup(expr any, sep ...string) Fd {
	str := "GROUP_CONCAT(%s)"
	if len(sep) > 0 {
		str = fmt.Sprintf("GROUP_CONCAT(%%s SEPARATOR '%s')", sep[0])
	}
	f := &Fd{
		v: []any{expr},
		s: str,
	}
	f.LabelHandler()
	return *f
}

func Round(expr any, num int) Fd {
	str := "ROUND(%s, %d)"
	f := &Fd{
		v: []any{expr, num},
		s: str,
	}
	f.LabelHandler()
	return *f
}

func cast(expr any, castType string) Fd {
	str := fmt.Sprintf("cast(%%s as %s)", castType)
	f := &Fd{
		v: []any{expr},
		s: str,
	}
	f.LabelHandler()
	return *f
}

// CastChar
func CastChar(expr any) Fd {
	return cast(expr, "CHAR")
}

// string or fields.Fd
func Concat(expr ...any) Fd {
	str := ""
	for _, value := range expr {
		switch value.(type) {
		case string:
			str += fmt.Sprintf("'%s',", value)
		case Fd:
			str += fmt.Sprintf("%s,", value.(Fd).String())
		}
	}
	if str != "" {
		str = str[:len(str)-1]
	}
	f := &Fd{
		v: []any{},
		s: fmt.Sprintf("concat(%s)", str),
	}
	f.LabelHandler()
	return *f
}

func Distinct(field ...any) Fd {
	f := &Fd{s: "DISTINCT %s", v: field}

	if f.values == nil {
		f.values = make(map[string]any)
	}
	bf := bytes.Buffer{}
	for _, vv := range field {
		switch vv.(type) {
		case Expr:
			val := vv.(Expr)
			bf.WriteString(val.String())
			bf.WriteString(",")
			if val.Values() != nil {
				for k, v := range *val.Values() {
					f.values[k] = v
				}
			}
		case Field:
			val := vv.(Field)
			bf.WriteString(val.String())
			bf.WriteString(",")
			if val.Values() != nil {
				for k, v := range *val.Values() {
					f.values[k] = v
				}
			}
		case string:
			bf.WriteString(vv.(string))
			bf.WriteString(",")
		default:
			panic("数据类型不正确, type must: Expr, Func, string")
		}
	}
	bf.Truncate(bf.Len() - 1)
	f.s = fmt.Sprintf(f.s, bf.String())

	//fd.ColumnNameHandler()
	//fd.s = fd.field
	return *f
}

func Now() Fd {
	f := &Fd{
		s: "NOW()",
		v: make([]any, 0),
	}
	f.LabelHandler()
	return *f
}

func DateSub(tm any, interval string) Fd {
	f := &Fd{s: "DATE_SUB(%s, " + interval + ")",
		v: []any{tm},
	}

	f.ColumnNameHandler()

	f.LabelHandler()
	return *f
}

func UnixTimeStamp(tm any) Fd {
	f := &Fd{s: "UNIX_TIMESTAMP(%s)",
		v: []any{tm},
	}
	f.ColumnNameHandler()

	f.LabelHandler()
	return *f
}

func Point(longitude, latitude any) Fd {
	f := &Fd{s: "point(%v, %v)",
		v: []any{longitude, latitude}}
	f.ColumnNameHandler()
	f.LabelHandler()
	return *f
}

func StDistanceSphere(point1, point2 any) Fd {
	f := &Fd{s: "st_distance_sphere(%v, %v)",
		v: []any{point1, point2}}
	f.ColumnNameHandler()
	f.LabelHandler()
	return *f
}

func Case(when []any, els any) Fd {
	if len(when) == 0 {
		panic("sql构造器case内容不能为空")
	}
	s := ""
	v := []any{}
	for _, item := range when {
		s += "%v\n"
		v = append(v, item)
	}
	if els != nil {
		s += " else %v"
		v = append(v, els)
	}
	f := &Fd{s: fmt.Sprintf(`
case 
	%s
end
`, s),
		v: v,
	}
	f.ColumnNameHandler()
	f.LabelHandler()
	return *f
}

func When(condition any, value any) Fd {
	f := &Fd{
		s: "when %v then %v",
		v: []any{condition, value},
	}
	f.ColumnNameHandler()
	f.LabelHandler()
	return *f
}

func Rand() Fd {
	f := &Fd{
		s: "Rand()",
		v: make([]any, 0),
	}
	f.ColumnNameHandler()
	f.LabelHandler()
	return *f
}
