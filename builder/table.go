package builder

import (
	"bytes"
	"fmt"
)

type table struct {
	Db         string // 所归属的数据库， 默认是不需要的
	Name       string
	ForceIndex string
}

func (t table) GetName() string {
	bf := bytes.Buffer{}
	if t.Db != "" {
		bf.WriteString(t.Db)
		bf.WriteString(".")
	}
	bf.WriteString(t.Name)
	return bf.String()
}

type join struct {
	JoinType string // left| right   默认 left
	Table    SqlBuilder
	Value    map[string]any
	On       Expr
}
type union struct {
	JoinType string
	Table    SqlBuilder
}

type SqlBuilder struct {
	Table      *table
	JoinTable  []join
	FieldParam []string

	WhereParam   []Expr
	LimitParam   int
	OffSetParams int

	OrderParam []Field

	GroupParam  []Field
	HavingParam Expr
	Label       string         // 设置这个了 会在 将整个查询用小括号包裹, 加上别名
	Values      map[string]any // 需要的参数 存储在这里

	UnionBuilder []union // union 操作
}

// As as 只有 表名 和 field , 查询结果 可以使用
func (s *SqlBuilder) As(label string) *SqlBuilder {
	s.Label = label
	return s
}

// Select
// fields 可以是 string | funcs.Field
func (s *SqlBuilder) Select(fields ...any) *SqlBuilder {
	for _, fd := range fields {
		switch fd.(type) {
		case string:
			s.FieldParam = append(s.FieldParam, fd.(string))
		case Field:
			s.FieldParam = append(s.FieldParam, fd.(Field).String())
		}
	}
	return s
}

// Field
// 获取字段
func (s *SqlBuilder) Field(field string, rename ...string) Fd {
	bf := bytes.Buffer{}
	if s.Label != "" {
		bf.WriteString(s.Label)
		bf.WriteString(".")
		bf.WriteString(field)
	} else {
		tableName := s.Table.GetName()
		if tableName == "" {
			bf.WriteString(field)
		} else {
			bf.WriteString(s.Table.GetName())
			bf.WriteString(".")
			bf.WriteString(field)
		}
	}

	fd := NewField(bf.String())

	return fd
}

func (s *SqlBuilder) ForceIndex(indexName string) *SqlBuilder {
	s.Table.ForceIndex = fmt.Sprintf("force index(%s)", indexName)
	return s
}

//[]common.Expr{
//	In("city", []string{"beijing", "shanghai"}),
//	Eq("score", 5),
//	Gt("age", 35),
//	IsNotNull("address"),

//}
// (((x1=? AND x2>=?) OR (x3=? AND x4!=?)) AND score=? AND city IN (?,?) AND age>? AND address IS NOT NULL)

func (s *SqlBuilder) Where(expr ...Expr) *SqlBuilder {
	s.WhereParam = append(s.WhereParam, expr...)
	return s
}

func (s *SqlBuilder) Group(group ...Field) *SqlBuilder {
	s.GroupParam = append(s.GroupParam, group...)
	return s
}

func (s *SqlBuilder) Having(ha Expr) *SqlBuilder {
	s.HavingParam = ha
	return s
}

func (s *SqlBuilder) Order(field ...Field) *SqlBuilder {
	s.OrderParam = append(s.OrderParam, field...)
	return s
}

func (s *SqlBuilder) Limit(limit int) *SqlBuilder {
	s.LimitParam = limit
	return s
}

func (s *SqlBuilder) OffSet(offset int) *SqlBuilder {
	s.OffSetParams = offset
	return s
}

func (s *SqlBuilder) First() *SqlBuilder {
	s.LimitParam = 1
	return s
}

// Join
// joinType  left|right
func (s *SqlBuilder) join(table SqlBuilder, on Expr, joinType string) *SqlBuilder {
	s.JoinTable = append(s.JoinTable, join{JoinType: joinType, Table: table, On: on})
	return s
}

func (s *SqlBuilder) LeftJoin(table *SqlBuilder, on Expr) *SqlBuilder {
	s.join(*table, on, "left")
	return s
}

func (s *SqlBuilder) RightJoin(table *SqlBuilder, on Expr) *SqlBuilder {
	s.join(*table, on, "right")
	return s
}

func (s *SqlBuilder) InnerJoin(table *SqlBuilder, on Expr) *SqlBuilder {
	s.join(*table, on, "inner")
	return s
}

func (s *SqlBuilder) Union(table *SqlBuilder, unionType ...string) *SqlBuilder {
	// union 也有
	// (table1) union (table2) union (table3)
	//	select * from (table1) union (table2) union (table3) order by sd desc limit 10
	//
	// UNION ALL (不去重) | UNION (去重)
	if len(unionType) == 0 {
		s.UnionBuilder = append(s.UnionBuilder, union{JoinType: "UNION ", Table: *table})
	} else {
		s.UnionBuilder = append(s.UnionBuilder, union{JoinType: unionType[0], Table: *table})
	}
	return s
}

// noDefault 不要默认的格式
func (s *SqlBuilder) getSelect(noDefault bool) (string, map[string]any) {
	bf := bytes.Buffer{}
	var value = map[string]any{}
	if !noDefault {
		bf.WriteString("select ")
	}
	if len(s.FieldParam) > 0 {
		for _, field := range s.FieldParam {
			bf.WriteString(field)
			bf.WriteString(", ")
		}
		bf.Truncate(bf.Len() - 2)
	} else {
		if !noDefault {
			bf.WriteString(" * ")
		}
	}
	return bf.String(), value
}

func (s *SqlBuilder) getTable() (string, map[string]any) {
	//if s.Table.GetName() == "" {
	//	panic("table name is empty")
	//}
	var value map[string]any = map[string]any{}

	bf := bytes.Buffer{}
	bf.WriteString(s.Table.GetName())

	if s.Table.ForceIndex != "" {
		bf.WriteString(" ")
		bf.WriteString(s.Table.ForceIndex)
	}
	if len(s.JoinTable) > 0 {
		for _, jt := range s.JoinTable {
			bf.WriteString(" ")
			bf.WriteString(jt.JoinType)
			bf.WriteString(" join ")
			tb, paramsData := jt.Table.subQuery() // 这里可能是 子查询
			if jt.Table.Label != "" {
				bf.WriteString("(")
				bf.WriteString(tb)
				bf.WriteString(") ")
				bf.WriteString(jt.Table.Label)
			} else {
				bf.WriteString(tb)
			}
			if paramsData != nil {
				for k, v := range paramsData {
					value[k] = v
				}
			}
			bf.WriteString(" on ")
			bf.WriteString(jt.On.String())
			if ve := jt.On.Values(); ve != nil {
				for k, v := range *ve {
					value[k] = v
				}
			}
		}
	}
	if len(s.UnionBuilder) > 0 {
		endLen := 0
		bf.WriteString("( ")
		for _, tb := range s.UnionBuilder {
			tbt, paramsData := tb.Table.Query()
			bf.WriteString("(")
			bf.WriteString(tbt)
			bf.WriteString(") ")
			bf.WriteString(tb.JoinType)
			endLen = len(tb.JoinType)
			for k, v := range paramsData {
				value[k] = v
			}
		}
		bf.Truncate(bf.Len() - endLen)
		bf.WriteString(" ) ")
		if s.Label != "" {
			bf.WriteString(s.Label)
		}
	}

	return bf.String(), value
}

func (s *SqlBuilder) getWhere() (string, map[string]any) {
	var value map[string]any = map[string]any{}
	bf := bytes.Buffer{}
	if s.WhereParam != nil && len(s.WhereParam) > 0 {
		bf.WriteString(" where ")
		for _, v := range s.WhereParam {
			bf.WriteString(v.String())
			bf.WriteString(" and ")

			if vau := v.Values(); vau != nil {
				for k, vi := range *vau {
					value[k] = vi
				}
			}
		}
		bf.Truncate(bf.Len() - 4)
	}
	return bf.String(), value
}

func (s *SqlBuilder) getGroupBy() (string, map[string]any) {
	bf := bytes.Buffer{}
	value := map[string]any{}

	if s.GroupParam != nil && len(s.GroupParam) > 0 {
		bf.WriteString(" GROUP BY ")
		for _, g := range s.GroupParam {
			bf.WriteString(g.String())
			bf.WriteString(",")
			if vl := g.Values(); vl != nil {
				for k, vi := range *vl {
					value[k] = vi
				}
			}
		}
		bf.Truncate(bf.Len() - 1)

		if s.HavingParam != nil {
			bf.WriteString(" HAVING ")
			bf.WriteString(s.HavingParam.String())
			if hvl := s.HavingParam.Values(); hvl != nil {
				for k, vi := range *hvl {
					value[k] = vi
				}
			}
		}
	}

	return bf.String(), value
}

func (s *SqlBuilder) getOrderBy() string {
	if s.OrderParam != nil && len(s.OrderParam) > 0 {
		bf := bytes.Buffer{}
		bf.WriteString(" order by ")
		for _, op := range s.OrderParam {
			bf.WriteString(op.String())
			bf.WriteString(", ")
		}
		bf.Truncate(bf.Len() - 2)
		return bf.String()
	}
	return ""
}

func (s *SqlBuilder) getLimit() string {
	if s.OffSetParams == 0 {
		if s.LimitParam > 0 {
			return fmt.Sprintf(" limit %d", s.LimitParam)
		} else {
			return ""
		}
	} else {
		return fmt.Sprintf(" limit %d, %d", s.LimitParam, s.OffSetParams)
	}
}

func (s *SqlBuilder) subQuery() (string, map[string]any) {
	var value = map[string]any{}
	bf := bytes.Buffer{}
	// select
	sl, data := s.getSelect(true)
	bf.WriteString(sl)
	for k, v := range data {
		value[k] = v
	}

	return s.commonQuery(bf, value)
}

func (s *SqlBuilder) Query() (string, map[string]any) {
	var value = map[string]any{}
	bf := bytes.Buffer{}
	// select
	sl, data := s.getSelect(false)
	bf.WriteString(sl)
	for k, v := range data {
		value[k] = v
	}
	return s.commonQuery(bf, value)
}

func (s *SqlBuilder) commonQuery(bf bytes.Buffer, value map[string]any) (string, map[string]any) {

	if bf.Len() > 0 {
		bf.WriteString(" from ")
	}
	// table
	sl, data := s.getTable()
	bf.WriteString(sl)
	for k, v := range data {
		value[k] = v
	}

	// where
	sl, data = s.getWhere()
	bf.WriteString(sl)
	for k, v := range data {
		value[k] = v
	}

	// group
	sl, data = s.getGroupBy()
	bf.WriteString(sl)
	for k, v := range data {
		value[k] = v
	}

	// order
	bf.WriteString(s.getOrderBy())

	// limit
	bf.WriteString(s.getLimit())
	return bf.String(), value
}
