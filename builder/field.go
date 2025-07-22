package builder

import (
	"bytes"
	"fmt"
)

// Field 将字符串变为 Fd类型
func NewField(field string) Fd {
	fd := &Fd{field: field, s: "%s", v: []any{field}}
	fd.ColumnNameHandler()
	fd.s = fd.field
	return *fd
}

type Fd struct {
	field  string
	values map[string]any
	v      []any
	s      string
	as     string
}

func (f Fd) Field() string {
	return f.field
}

func (f Fd) String() string {
	return f.s
}
func (f Fd) Values() *map[string]any {
	return &f.values
}
func (f Fd) As(label string) Field {
	f.as = label
	bf := bytes.Buffer{}
	bf.WriteString(f.s)
	bf.WriteString(" as ")
	bf.WriteString(f.as)
	f.s = bf.String()
	return f
}

func (f *Fd) ColumnNameHandler() {
	if f.field != "" {
		f.field = ColumnNameHandler(f.field)
	}
}

func (f *Fd) LabelHandler() {
	var value []any = make([]any, 0)
	if f.values == nil {
		f.values = make(map[string]any)
	}
	for _, vv := range f.v {
		if vv == nil {
			value = append(value, "null")
			continue
		}
		switch vv.(type) {
		case Expr:
			val := vv.(Expr)
			value = append(value, val.String())
			if val.Values() != nil {
				for k, v := range *val.Values() {
					f.values[k] = v
				}
			}
		case Field:
			val := vv.(Field)
			value = append(value, val.String())
			if val.Values() != nil {
				for k, v := range *val.Values() {
					f.values[k] = v
				}
			}
		case string:
			value = append(value, "'"+vv.(string)+"'")
		default:

			value = append(value, vv)

		}
	}
	f.s = fmt.Sprintf(f.s, value...)
	return
}

func (f Fd) Desc() Fd {
	if f.field != "" {
		f.s = f.s + " DESC"
		f.v = append(f.v, "DESC")
	}
	return f
}
func (f Fd) Asc() Fd {
	if f.field != "" {
		f.s = f.s + " ASC"
		f.v = append(f.v, "ASC")
	}
	return f
}

func (f Fd) Eq(value any, key ...string) Expr {
	return eq(f.String(), value, key...)
}
func (f Fd) NotEq(value any, key ...string) Expr {
	return notEq(f.String(), value, key...)
}

func (f Fd) Lte(value any, key ...string) Expr {
	return lte(f.String(), value, key...)
}
func (f Fd) Lt(value any, key ...string) Expr {
	return lt(f.String(), value, key...)
}
func (f Fd) Gte(value any, key ...string) Expr {
	return gte(f.String(), value, key...)
}
func (f Fd) Gt(value any, key ...string) Expr {
	return gt(f.String(), value, key...)
}

func (f Fd) IsNull() Expr {
	return isNull(f.String())
}

func (f Fd) IsNotNull() Expr {
	return isNotNull(f.String())
}

func (f Fd) In(d any, key ...string) Expr {
	return in(f.String(), d, key...)
}
func (f Fd) NotIn(d any, key ...string) Expr {
	return notIn(f.String(), d, key...)
}

func (f Fd) Distinct() Fd {
	fs := Fd{s: "DISTINCT(%v)", v: []any{f}}
	fs.LabelHandler()
	return fs
}

func (f Fd) Count() Fd {
	return Count(f)
}

// 乘 整数或浮点小数
func (f Fd) Mul(value any) Fd {
	fs := &Fd{
		v: []any{f},
		s: fmt.Sprintf("%%v * %v", value),
	}
	fs.LabelHandler()
	return *fs
}

// 加 值
func (f Fd) Add(value any) Fd {
	fs := &Fd{
		v: []any{f},
		s: fmt.Sprintf("%%v + %v", value),
	}
	fs.LabelHandler()
	return *fs
}

// 加 字段
func (f Fd) AddCol(value Fd) Fd {
	fs := &Fd{
		v: []any{f, value},
		s: "%v + %v",
	}
	fs.LabelHandler()
	return *fs
}

// 除 值
func (f Fd) Div(value any) Fd {
	fs := &Fd{
		v: []any{f},
		s: fmt.Sprintf("%%v / %v", value),
	}
	fs.LabelHandler()
	return *fs
}

// 除 字段
func (f Fd) DivCol(value Fd) Fd {
	fs := &Fd{
		v: []any{f, value},
		s: "%v / %v",
	}
	fs.LabelHandler()
	return *fs
}

// 减 值
func (f Fd) Sub(value any) Fd {
	fs := &Fd{
		v: []any{f},
		s: fmt.Sprintf("%%v - %v", value),
	}
	fs.LabelHandler()
	return *fs
}

// 减 值
func (f Fd) BeSub(value any) Fd {
	fs := &Fd{
		v: []any{f},
		s: fmt.Sprintf("%v - %%v", value),
	}
	fs.LabelHandler()
	return *fs
}

// 减 字段
func (f Fd) SubCol(value Fd) Fd {
	fs := &Fd{
		v: []any{f, value},
		s: "%v - %v",
	}
	fs.LabelHandler()
	return *fs
}

func (f Fd) Like(value string) Expr {
	return like(f.String(), value)
}

func (f Fd) Max() Fd {
	ff := &Fd{
		v: []any{f},
		s: "MAX(%v)",
	}
	ff.ColumnNameHandler()
	ff.LabelHandler()
	return *ff
}
