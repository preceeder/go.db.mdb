package builder

import (
	"bytes"
	"sort"
	"strconv"
	"strings"
)

type table struct {
	Db         string // 所归属的数据库， 默认是不需要的
	Name       string
	Label      string
	ForceIndex string
}

func (t table) GetName() string {
	bf := bytes.Buffer{}
	if t.Db != "" {
		bf.WriteString(t.Db)
		bf.WriteString(".")
	}
	bf.WriteString(t.Name)
	//bf.WriteString(" ")
	return bf.String()
}

type join struct {
	JoinType string // left| right   默认 left
	SubTable SqlBuilder
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

	WhereParam  []Expr
	LimitParam  int
	OffsetParam int // 修复命名: OffSetParams -> OffsetParam

	OrderParam []Field

	GroupParam  []Field
	HavingParam Expr
	label       string         // 设置这个了 会在 将整个查询用小括号包裹, 加上别名
	values      map[string]any // 需要的参数 存储在这里

	UnionBuilder []union // union 操作

	insertColumns []string
	insertValues  []any

	// data holders for DML
	// 不强制使用这些字段，提供便捷 DML 构建方法

    // FromSubQuery 表示该查询的 FROM 来源是一个子查询
    FromSubQuery *SqlBuilder
}

// As 设置表的别名
func (s *SqlBuilder) As(label string) *SqlBuilder {
	s.Table.Label = label
	return s
}

// Label 设置子查询的别名
// 注意：这个需要在子查询的最后使用，不要提前使用，不然会和上面的 As 有冲突
func (s *SqlBuilder) Label(label string) *SqlBuilder {
	s.label = label
	return s
}

// FromSub 将另一个构建器作为当前查询的 FROM 子查询来源
// 示例：
//   sub := Table("t").Select(...).Group(...).Label("sub")
//   q := Table("").FromSub(sub).Select(q.Field("col")).Query()
func (s *SqlBuilder) FromSub(sub *SqlBuilder) *SqlBuilder {
    s.FromSubQuery = sub
    if sub != nil && sub.label != "" {
        s.label = sub.label
    }
    return s
}

// Select 设置查询的字段列表
// fields 可以是 string 或 Field 类型
func (s *SqlBuilder) Select(fields ...any) *SqlBuilder {
	for _, fd := range fields {
		switch val := fd.(type) {
		case string:
			s.FieldParam = append(s.FieldParam, val)
		case Field:
			s.FieldParam = append(s.FieldParam, val.String())
		}
	}
	return s
}

// Field
// 获取字段
func (s *SqlBuilder) Field(field string) Fd {
	bf := bytes.Buffer{}
	if s.label != "" {
		bf.WriteString(s.label)
		bf.WriteString(".")
		bf.WriteString(field)
	} else if s.Table.Label != "" {
		bf.WriteString(s.Table.Label)
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

// ForceIndex 强制使用指定的索引
// 注意：使用此方法会强制 MySQL 使用指定索引，可能导致性能问题
func (s *SqlBuilder) ForceIndex(indexName string) *SqlBuilder {
	var bf bytes.Buffer
	bf.WriteString("FORCE INDEX(`")
	bf.WriteString(indexName)
	bf.WriteString("`)")
	s.Table.ForceIndex = bf.String()
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

// Offset 设置查询的偏移量（用于分页）
func (s *SqlBuilder) Offset(offset int) *SqlBuilder {
	s.OffsetParam = offset
	return s
}

// OffSet 设置查询的偏移量（已废弃，保留用于向后兼容，请使用 Offset）
// Deprecated: 使用 Offset 替代
func (s *SqlBuilder) OffSet(offset int) *SqlBuilder {
	return s.Offset(offset)
}

func (s *SqlBuilder) First() *SqlBuilder {
	s.LimitParam = 1
	return s
}

// Join
// joinType  left|right
func (s *SqlBuilder) join(table SqlBuilder, on Expr, joinType string) *SqlBuilder {
	s.JoinTable = append(s.JoinTable, join{JoinType: joinType, SubTable: table, On: on})
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

func (s *SqlBuilder) getDelete(t ...*SqlBuilder) string {
	bf := bytes.Buffer{}
	bf.WriteString("DELETE ")

	if len(t) > 0 {
		if t[0].label != "" {
			bf.WriteString(t[0].label)
		} else if t[0].Table.Label != "" {
			bf.WriteString(t[0].Table.Label)
		} else {
			bf.WriteString(t[0].Table.GetName())
		}
	}
	return bf.String()
}

func (s *SqlBuilder) getInsert() string {
	bf := bytes.Buffer{}
	bf.WriteString("INSERT INTO ")
	bf.WriteString(s.Table.GetName())
	return bf.String()
}

// InsertMap 构建单行插入 SQL：INSERT INTO table (`a`,`b`) VALUES (:a,:b)
func (s *SqlBuilder) InsertMap(data map[string]any) (string, map[string]any) {
	if s.Table == nil || s.Table.GetName() == "" {
		return "", nil
	}
	if len(data) == 0 {
		return "", nil
	}

	cols := make([]string, 0, len(data))
	phs := make([]string, 0, len(data))
	params := make(map[string]any, len(data))
	for k, v := range data {
		cols = append(cols, ColumnNameHandler(k))
		phs = append(phs, ":"+k)
		params[k] = v
	}
	var bf bytes.Buffer
	bf.WriteString(s.getInsert())
	bf.WriteString(" (")
	bf.WriteString(strings.Join(cols, ", "))
	bf.WriteString(") VALUES (")
	bf.WriteString(strings.Join(phs, ", "))
	bf.WriteString(")")
	return bf.String(), params
}

// InsertMany 构建多行插入 SQL：INSERT INTO table (`a`,`b`) VALUES (:a_0,:b_0),(:a_1,:b_1)...
func (s *SqlBuilder) InsertMany(rows []map[string]any) (string, map[string]any) {
	if s.Table == nil || s.Table.GetName() == "" {
		return "", nil
	}
	if len(rows) == 0 {
		return "", nil
	}
	// 以第一行确定列顺序
	first := rows[0]
	if len(first) == 0 {
		return "", nil
	}
	cols := make([]string, 0, len(first))
	for k := range first {
		cols = append(cols, k)
	}
	// 稳定列顺序（按名称排序）
	sort.Strings(cols)
	quotedCols := make([]string, len(cols))
	for i, c := range cols {
		quotedCols[i] = ColumnNameHandler(c)
	}

	params := make(map[string]any, len(rows)*len(cols))
	valuesTuples := make([]string, 0, len(rows))
	for i, row := range rows {
		placeholders := make([]string, 0, len(cols))
		for _, c := range cols {
			key := c + "_" + strconv.Itoa(i)
			placeholders = append(placeholders, ":"+key)
			if v, ok := row[c]; ok {
				params[key] = v
			} else {
				params[key] = nil
			}
		}
		valuesTuples = append(valuesTuples, "("+strings.Join(placeholders, ", ")+")")
	}

	var bf bytes.Buffer
	bf.WriteString(s.getInsert())
	bf.WriteString(" (")
	bf.WriteString(strings.Join(quotedCols, ", "))
	bf.WriteString(") VALUES ")
	bf.WriteString(strings.Join(valuesTuples, ", "))
	return bf.String(), params
}

// UpdateMap 构建更新 SQL：UPDATE <table [JOIN ...]> SET `a`=:a,`b`=:b WHERE ...
func (s *SqlBuilder) UpdateMap(set map[string]any) (string, map[string]any) {
	if s.Table == nil || s.Table.GetName() == "" {
		return "", nil
	}
	if len(set) == 0 {
		return "", nil
	}
	// 表与 Join
	tbl, tblParams := s.getTable()
	// SET 子句
	setParts := make([]string, 0, len(set))
	params := make(map[string]any, len(set))
	for k, v := range set {
		setParts = append(setParts, ColumnNameHandler(k)+" = :"+k)
		params[k] = v
	}
	// WHERE 子句
	whereStr, whereParams := s.getWhere()
	for k, v := range tblParams {
		params[k] = v
	}
	for k, v := range whereParams {
		params[k] = v
	}

	var bf bytes.Buffer
	bf.WriteString("UPDATE ")
	bf.WriteString(tbl)
	bf.WriteString(" SET ")
	bf.WriteString(strings.Join(setParts, ", "))
	bf.WriteString(whereStr)
	return bf.String(), params
}

// UpdateOrdered 构建更新 SQL（按顺序设置 SET 列）
// 例如：[]map[string]any{{"name":"a"},{"age":2}} 会按给定顺序生成 SET 子句
func (s *SqlBuilder) UpdateOrdered(orderedSet []map[string]any) (string, map[string]any) {
    if s.Table == nil || s.Table.GetName() == "" {
        return "", nil
    }
    if len(orderedSet) == 0 {
        return "", nil
    }
    // 表与 Join
    tbl, tblParams := s.getTable()
    // SET 子句（保持顺序）
    setParts := make([]string, 0, len(orderedSet))
    params := make(map[string]any, len(orderedSet))
    for _, item := range orderedSet {
        for k, v := range item {
            setParts = append(setParts, ColumnNameHandler(k)+" = :"+k)
            params[k] = v
        }
    }
    // WHERE 子句
    whereStr, whereParams := s.getWhere()
    for k, v := range tblParams {
        params[k] = v
    }
    for k, v := range whereParams {
        params[k] = v
    }

    var bf bytes.Buffer
    bf.WriteString("UPDATE ")
    bf.WriteString(tbl)
    bf.WriteString(" SET ")
    bf.WriteString(strings.Join(setParts, ", "))
    bf.WriteString(whereStr)
    return bf.String(), params
}

// InsertOnDuplicateCols 构建 Upsert：指定插入列及冲突时用 VALUES(col) 更新的列
// INSERT INTO t (...) VALUES (...) ON DUPLICATE KEY UPDATE col=VALUES(col), ...
func (s *SqlBuilder) InsertOnDuplicateCols(set map[string]any, updateCols []string) (string, map[string]any) {
    if s.Table == nil || s.Table.GetName() == "" {
        return "", nil
    }
    if len(set) == 0 || len(updateCols) == 0 {
        return "", nil
    }
    // 基础 insert
    cols := make([]string, 0, len(set))
    phs := make([]string, 0, len(set))
    params := make(map[string]any, len(set))
    for k, v := range set {
        cols = append(cols, ColumnNameHandler(k))
        phs = append(phs, ":"+k)
        params[k] = v
    }
    var bf bytes.Buffer
    bf.WriteString(s.getInsert())
    bf.WriteString(" (")
    bf.WriteString(strings.Join(cols, ", "))
    bf.WriteString(") VALUES (")
    bf.WriteString(strings.Join(phs, ", "))
    bf.WriteString(") ON DUPLICATE KEY UPDATE ")

    upd := make([]string, 0, len(updateCols))
    for _, c := range updateCols {
        upd = append(upd, ColumnNameHandler(c)+"=VALUES("+ColumnNameHandler(c)+")")
    }
    bf.WriteString(strings.Join(upd, ", "))
    return bf.String(), params
}

// InsertOnDuplicateMap 构建 Upsert：指定插入列及冲突时按 map 指定值更新
// INSERT INTO t (...) VALUES (...) ON DUPLICATE KEY UPDATE col=:col_upd, ...
func (s *SqlBuilder) InsertOnDuplicateMap(set map[string]any, update map[string]any) (string, map[string]any) {
    if s.Table == nil || s.Table.GetName() == "" {
        return "", nil
    }
    if len(set) == 0 || len(update) == 0 {
        return "", nil
    }
    // 基础 insert
    cols := make([]string, 0, len(set))
    phs := make([]string, 0, len(set))
    params := make(map[string]any, len(set)+len(update))
    for k, v := range set {
        cols = append(cols, ColumnNameHandler(k))
        phs = append(phs, ":"+k)
        params[k] = v
    }
    var bf bytes.Buffer
    bf.WriteString(s.getInsert())
    bf.WriteString(" (")
    bf.WriteString(strings.Join(cols, ", "))
    bf.WriteString(") VALUES (")
    bf.WriteString(strings.Join(phs, ", "))
    bf.WriteString(") ON DUPLICATE KEY UPDATE ")

    upd := make([]string, 0, len(update))
    for k, v := range update {
        key := k + "_upd"
        upd = append(upd, ColumnNameHandler(k)+" = :"+key)
        params[key] = v
    }
    bf.WriteString(strings.Join(upd, ", "))
    return bf.String(), params
}

// noDefault 不要默认的格式
func (s *SqlBuilder) getSelect(noDefault bool) (string, map[string]any) {
	var value = map[string]any{}

	if len(s.FieldParam) > 0 {
		// 使用 strings.Join 代替循环+Truncate，性能更好
		return "SELECT " + strings.Join(s.FieldParam, ", "), value
	}

	if !noDefault {
		return "SELECT *", value
	}
	return "", value
}

func (s *SqlBuilder) getTable() (string, map[string]any) {
	//if s.Table.GetName() == "" {
	//	panic("table name is empty")
	//}
	var value map[string]any = map[string]any{}

    bf := bytes.Buffer{}
    // 优先使用子查询作为 FROM 来源
    if s.FromSubQuery != nil {
        tb, paramsData := s.FromSubQuery.subQuery()
        bf.WriteString("(")
        bf.WriteString(tb)
        bf.WriteString(") ")
        if s.label != "" {
            bf.WriteString(s.label)
        }
        if paramsData != nil {
            for k, v := range paramsData {
                value[k] = v
            }
        }
    } else {
        bf.WriteString(s.Table.GetName())
    }

	if s.Table.Label != "" {
		bf.WriteString(" AS ")
		bf.WriteString(s.Table.Label)
	}

	if s.Table.ForceIndex != "" {
		bf.WriteString(" ")
		bf.WriteString(s.Table.ForceIndex)
	}

	if len(s.JoinTable) > 0 {
		for _, jt := range s.JoinTable {
			bf.WriteString(" ")
			bf.WriteString(jt.JoinType)
			bf.WriteString(" join ")
			tb, paramsData := jt.SubTable.subQuery() // 这里可能是 子查询
			if jt.SubTable.label != "" {
				bf.WriteString("(")
				bf.WriteString(tb)
				bf.WriteString(") ")
				bf.WriteString(jt.SubTable.label)
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
		if s.label != "" {
			bf.WriteString(s.label)
		}
	}

	return bf.String(), value
}

func (s *SqlBuilder) getWhere() (string, map[string]any) {
	var value map[string]any = map[string]any{}
	if len(s.WhereParam) == 0 {
		return "", value
	}

	bf := bytes.Buffer{}
	bf.WriteString(" WHERE ")
	for _, v := range s.WhereParam {
		if vau := v.Values(); vau != nil {
			for k, vi := range *vau {
				value[k] = vi
			}
		}
		bf.WriteString(v.String())
		bf.WriteString(" AND ")
	}
	// 移除最后一个 " AND "
	if bf.Len() > 6 {
		bf.Truncate(bf.Len() - 5)
	}
	return bf.String(), value
}

func (s *SqlBuilder) getGroupBy() (string, map[string]any) {
	value := map[string]any{}
	if len(s.GroupParam) == 0 {
		return "", value
	}

	// 先收集参数和字段字符串
	parts := make([]string, 0, len(s.GroupParam))
	for _, g := range s.GroupParam {
		if vl := g.Values(); vl != nil {
			for k, vi := range *vl {
				value[k] = vi
			}
		}
		parts = append(parts, g.String())
	}

	var bf bytes.Buffer
	bf.WriteString(" GROUP BY ")
	bf.WriteString(strings.Join(parts, ", "))

	if s.HavingParam != nil {
		bf.WriteString(" HAVING ")
		bf.WriteString(s.HavingParam.String())
		if hvl := s.HavingParam.Values(); hvl != nil {
			for k, vi := range *hvl {
				value[k] = vi
			}
		}
	}

	return bf.String(), value
}

func (s *SqlBuilder) getOrderBy() string {
	if len(s.OrderParam) == 0 {
		return ""
	}

	// 预分配切片，使用 strings.Join 提高性能
	parts := make([]string, 0, len(s.OrderParam))
	for _, op := range s.OrderParam {
		parts = append(parts, op.String())
	}
	return " ORDER BY " + strings.Join(parts, ", ")
}

func (s *SqlBuilder) getLimit() string {
	if s.OffsetParam == 0 {
		if s.LimitParam > 0 {
			var bf bytes.Buffer
			bf.WriteString(" LIMIT ")
			bf.WriteString(strconv.Itoa(s.LimitParam))
			return bf.String()
		}
		return ""
	}
	// MySQL LIMIT 语法: LIMIT offset, limit
	var bf bytes.Buffer
	bf.WriteString(" LIMIT ")
	bf.WriteString(strconv.Itoa(s.OffsetParam))
	bf.WriteString(", ")
	bf.WriteString(strconv.Itoa(s.LimitParam))
	return bf.String()
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

func (s *SqlBuilder) commonQuery(bf bytes.Buffer, value map[string]any) (string, map[string]any) {
	if bf.Len() > 0 {
		bf.WriteString(" FROM ")
	}

	// 收集所有需要合并的 map，减少多次遍历
	var mapsToMerge []map[string]any

	// table
	sl, data := s.getTable()
	bf.WriteString(sl)
	if len(data) > 0 {
		mapsToMerge = append(mapsToMerge, data)
	}

	// where
	sl, data = s.getWhere()
	bf.WriteString(sl)
	if len(data) > 0 {
		mapsToMerge = append(mapsToMerge, data)
	}

	// group
	sl, data = s.getGroupBy()
	bf.WriteString(sl)
	if len(data) > 0 {
		mapsToMerge = append(mapsToMerge, data)
	}

	// order
	bf.WriteString(s.getOrderBy())

	// limit
	bf.WriteString(s.getLimit())

	// 一次性合并所有 map，减少遍历次数
	if len(mapsToMerge) > 0 {
		// 计算总容量
		totalSize := len(value) // value 已有容量
		for _, m := range mapsToMerge {
			totalSize += len(m)
		}
		// 如果 value 为 nil，初始化；否则确保有足够容量
		if value == nil {
			value = make(map[string]any, totalSize)
		}
		// 合并所有 map
		for _, m := range mapsToMerge {
			for k, v := range m {
				value[k] = v
			}
		}
	}

	return bf.String(), value
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

// Delete 构建 DELETE 查询语句
// t 可选参数，指定要删除的表（用于多表 JOIN 删除的场景）
// 示例:
//   - Delete() -> DELETE FROM table WHERE ...
//   - Delete(&table) -> DELETE table_alias FROM table WHERE ...
func (s *SqlBuilder) Delete(t ...*SqlBuilder) (string, map[string]any) {
	var value = map[string]any{}
	bf := bytes.Buffer{}
	sl := s.getDelete(t...)
	bf.WriteString(sl)
	return s.commonQuery(bf, value)
}
