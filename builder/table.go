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

	// DML 操作相关字段
	dmlType string      // DML 操作类型: "insert", "insert_many", "update", "update_ordered", "insert_on_duplicate_cols", "insert_on_duplicate_map", "delete"
	dmlData interface{} // DML 操作数据
	deleteTarget *SqlBuilder // Delete 操作的删除目标表（可选）
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
//   q := Table("").FromSub(sub).Select(q.Field("col")).Sql()
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

func (s *SqlBuilder) getDelete(t *SqlBuilder) string {
	bf := bytes.Buffer{}
	bf.WriteString("DELETE ")

	if t != nil {
		if t.label != "" {
			bf.WriteString(t.label)
		} else if t.Table.Label != "" {
			bf.WriteString(t.Table.Label)
		} else {
			bf.WriteString(t.Table.GetName())
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

// InsertMap 设置单行插入操作，返回 *SqlBuilder 以支持链式调用
// 使用: sql, params := builder.Table("user").InsertMap(data).Sql()
func (s *SqlBuilder) InsertMap(data map[string]any) *SqlBuilder {
	s.dmlType = "insert"
	s.dmlData = data
	return s
}

// buildInsertMap 构建单行插入 SQL：INSERT INTO table (`a`,`b`) VALUES (:a,:b)
func (s *SqlBuilder) buildInsertMap() (string, map[string]any) {
	if s.Table == nil || s.Table.GetName() == "" {
		return "", nil
	}
	data, ok := s.dmlData.(map[string]any)
	if !ok || len(data) == 0 {
		return "", nil
	}

	cols := make([]string, 0, len(data))
	phs := make([]string, 0, len(data))
	insertParams := make(map[string]any, len(data))
	for k, v := range data {
		cols = append(cols, ColumnNameHandler(k))
		phs = append(phs, ":"+k)
		insertParams[k] = v
	}
	
	// 使用统一的参数合并方法
	params := mergeParams(insertParams)
	
	var bf bytes.Buffer
	bf.WriteString(s.getInsert())
	bf.WriteString(" (")
	bf.WriteString(strings.Join(cols, ", "))
	bf.WriteString(") VALUES (")
	bf.WriteString(strings.Join(phs, ", "))
	bf.WriteString(")")
	return bf.String(), params
}

// InsertMany 设置多行插入操作，返回 *SqlBuilder 以支持链式调用
// 使用: sql, params := builder.Table("user").InsertMany(rows).Sql()
func (s *SqlBuilder) InsertMany(rows []map[string]any) *SqlBuilder {
	s.dmlType = "insert_many"
	s.dmlData = rows
	return s
}

// buildInsertMany 构建多行插入 SQL：INSERT INTO table (`a`,`b`) VALUES (:a_0,:b_0),(:a_1,:b_1)...
func (s *SqlBuilder) buildInsertMany() (string, map[string]any) {
	if s.Table == nil || s.Table.GetName() == "" {
		return "", nil
	}
	rows, ok := s.dmlData.([]map[string]any)
	if !ok || len(rows) == 0 {
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

	insertParams := make(map[string]any, len(rows)*len(cols))
	valuesTuples := make([]string, 0, len(rows))
	for i, row := range rows {
		placeholders := make([]string, 0, len(cols))
		for _, c := range cols {
			key := c + "_" + strconv.Itoa(i)
			placeholders = append(placeholders, ":"+key)
			if v, ok := row[c]; ok {
				insertParams[key] = v
			} else {
				insertParams[key] = nil
			}
		}
		valuesTuples = append(valuesTuples, "("+strings.Join(placeholders, ", ")+")")
	}

	// 使用统一的参数合并方法
	params := mergeParams(insertParams)

	var bf bytes.Buffer
	bf.WriteString(s.getInsert())
	bf.WriteString(" (")
	bf.WriteString(strings.Join(quotedCols, ", "))
	bf.WriteString(") VALUES ")
	bf.WriteString(strings.Join(valuesTuples, ", "))
	return bf.String(), params
}

// UpdateMap 设置更新操作，返回 *SqlBuilder 以支持链式调用
// 使用: sql, params := builder.Table("user").Where(...).UpdateMap(data).Sql()
func (s *SqlBuilder) UpdateMap(set map[string]any) *SqlBuilder {
	s.dmlType = "update"
	s.dmlData = set
	return s
}

// buildUpdateMap 构建更新 SQL：UPDATE <table [JOIN ...]> SET `a`=:a,`b`=:b WHERE ...
func (s *SqlBuilder) buildUpdateMap() (string, map[string]any) {
	if s.Table == nil || s.Table.GetName() == "" {
		return "", nil
	}
	set, ok := s.dmlData.(map[string]any)
	if !ok || len(set) == 0 {
		return "", nil
	}
	// 表与 Join
	tbl, tblParams := s.getTable()
	// SET 子句
	setParts := make([]string, 0, len(set))
	setParams := make(map[string]any, len(set))
	for k, v := range set {
		setParts = append(setParts, ColumnNameHandler(k)+" = :"+k)
		setParams[k] = v
	}
	// WHERE 子句
	whereStr, whereParams := s.getWhere()
	
	// 使用统一的参数合并方法
	params := mergeParams(setParams, tblParams, whereParams)

	var bf bytes.Buffer
	bf.WriteString("UPDATE ")
	bf.WriteString(tbl)
	bf.WriteString(" SET ")
	bf.WriteString(strings.Join(setParts, ", "))
	bf.WriteString(whereStr)
	return bf.String(), params
}

// UpdateOrdered 设置更新操作（按顺序设置 SET 列），返回 *SqlBuilder 以支持链式调用
// 例如：[]map[string]any{{"name":"a"},{"age":2}} 会按给定顺序生成 SET 子句
// 使用: sql, params := builder.Table("user").Where(...).UpdateOrdered(orderedSet).Sql()
func (s *SqlBuilder) UpdateOrdered(orderedSet []map[string]any) *SqlBuilder {
	s.dmlType = "update_ordered"
	s.dmlData = orderedSet
	return s
}

// buildUpdateOrdered 构建更新 SQL（按顺序设置 SET 列）
func (s *SqlBuilder) buildUpdateOrdered() (string, map[string]any) {
	if s.Table == nil || s.Table.GetName() == "" {
		return "", nil
	}
	orderedSet, ok := s.dmlData.([]map[string]any)
	if !ok || len(orderedSet) == 0 {
		return "", nil
	}
	// 表与 Join
	tbl, tblParams := s.getTable()
	// SET 子句（保持顺序）
	setParts := make([]string, 0, len(orderedSet))
	setParams := make(map[string]any)
	for _, item := range orderedSet {
		for k, v := range item {
			setParts = append(setParts, ColumnNameHandler(k)+" = :"+k)
			setParams[k] = v
		}
	}
	// WHERE 子句
	whereStr, whereParams := s.getWhere()
	
	// 使用统一的参数合并方法
	params := mergeParams(setParams, tblParams, whereParams)

	var bf bytes.Buffer
	bf.WriteString("UPDATE ")
	bf.WriteString(tbl)
	bf.WriteString(" SET ")
	bf.WriteString(strings.Join(setParts, ", "))
	bf.WriteString(whereStr)
	return bf.String(), params
}

// InsertOnDuplicateColsData 存储 InsertOnDuplicateCols 的数据
type InsertOnDuplicateColsData struct {
	Set       map[string]any
	UpdateCols []string
}

// InsertOnDuplicateCols 设置 Upsert 操作，返回 *SqlBuilder 以支持链式调用
// 指定插入列及冲突时用 VALUES(col) 更新的列
// 使用: sql, params := builder.Table("user").InsertOnDuplicateCols(set, updateCols).Sql()
func (s *SqlBuilder) InsertOnDuplicateCols(set map[string]any, updateCols []string) *SqlBuilder {
	s.dmlType = "insert_on_duplicate_cols"
	s.dmlData = InsertOnDuplicateColsData{
		Set:        set,
		UpdateCols: updateCols,
	}
	return s
}

// buildInsertOnDuplicateCols 构建 Upsert SQL：指定插入列及冲突时用 VALUES(col) 更新的列
func (s *SqlBuilder) buildInsertOnDuplicateCols() (string, map[string]any) {
	if s.Table == nil || s.Table.GetName() == "" {
		return "", nil
	}
	data, ok := s.dmlData.(InsertOnDuplicateColsData)
	if !ok || len(data.Set) == 0 || len(data.UpdateCols) == 0 {
		return "", nil
	}
	// 基础 insert
	cols := make([]string, 0, len(data.Set))
	phs := make([]string, 0, len(data.Set))
	insertParams := make(map[string]any, len(data.Set))
	for k, v := range data.Set {
		cols = append(cols, ColumnNameHandler(k))
		phs = append(phs, ":"+k)
		insertParams[k] = v
	}
	
	// 使用统一的参数合并方法
	params := mergeParams(insertParams)
	
	var bf bytes.Buffer
	bf.WriteString(s.getInsert())
	bf.WriteString(" (")
	bf.WriteString(strings.Join(cols, ", "))
	bf.WriteString(") VALUES (")
	bf.WriteString(strings.Join(phs, ", "))
	bf.WriteString(") ON DUPLICATE KEY UPDATE ")

	upd := make([]string, 0, len(data.UpdateCols))
	for _, c := range data.UpdateCols {
		upd = append(upd, ColumnNameHandler(c)+"=VALUES("+ColumnNameHandler(c)+")")
	}
	bf.WriteString(strings.Join(upd, ", "))
	return bf.String(), params
}

// InsertOnDuplicateMapData 存储 InsertOnDuplicateMap 的数据
type InsertOnDuplicateMapData struct {
	Set    map[string]any
	Update map[string]any
}

// InsertOnDuplicateMap 设置 Upsert 操作，返回 *SqlBuilder 以支持链式调用
// 指定插入列及冲突时按 map 指定值更新
// 使用: sql, params := builder.Table("user").InsertOnDuplicateMap(set, update).Sql()
func (s *SqlBuilder) InsertOnDuplicateMap(set map[string]any, update map[string]any) *SqlBuilder {
	s.dmlType = "insert_on_duplicate_map"
	s.dmlData = InsertOnDuplicateMapData{
		Set:    set,
		Update: update,
	}
	return s
}

// buildInsertOnDuplicateMap 构建 Upsert SQL：指定插入列及冲突时按 map 指定值更新
func (s *SqlBuilder) buildInsertOnDuplicateMap() (string, map[string]any) {
	if s.Table == nil || s.Table.GetName() == "" {
		return "", nil
	}
	data, ok := s.dmlData.(InsertOnDuplicateMapData)
	if !ok || len(data.Set) == 0 || len(data.Update) == 0 {
		return "", nil
	}
	// 基础 insert
	cols := make([]string, 0, len(data.Set))
	phs := make([]string, 0, len(data.Set))
	insertParams := make(map[string]any, len(data.Set))
	for k, v := range data.Set {
		cols = append(cols, ColumnNameHandler(k))
		phs = append(phs, ":"+k)
		insertParams[k] = v
	}
	
	// 处理 UPDATE 部分的参数
	updateParams := make(map[string]any, len(data.Update))
	upd := make([]string, 0, len(data.Update))
	for k, v := range data.Update {
		key := k + "_upd"
		upd = append(upd, ColumnNameHandler(k)+" = :"+key)
		updateParams[key] = v
	}
	
	// 使用统一的参数合并方法
	params := mergeParams(insertParams, updateParams)
	
	var bf bytes.Buffer
	bf.WriteString(s.getInsert())
	bf.WriteString(" (")
	bf.WriteString(strings.Join(cols, ", "))
	bf.WriteString(") VALUES (")
	bf.WriteString(strings.Join(phs, ", "))
	bf.WriteString(") ON DUPLICATE KEY UPDATE ")
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
        } else {
            bf.WriteString("sub")
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
			tbt, paramsData := tb.Table.Sql()
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
        } else {
            bf.WriteString("sub")
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

// mergeParams 统一合并参数的方法，与 commonQuery 保持一致
// 将多个参数 map 合并到一个 map 中，预分配容量以提高性能
func mergeParams(baseParams map[string]any, additionalParams ...map[string]any) map[string]any {
	// 计算总容量
	totalSize := len(baseParams)
	for _, m := range additionalParams {
		totalSize += len(m)
	}

	// 如果 baseParams 为 nil，初始化；否则确保有足够容量
	if baseParams == nil {
		baseParams = make(map[string]any, totalSize)
	} else if totalSize > len(baseParams) {
		// 如果需要扩容，创建新 map 并复制现有数据
		newParams := make(map[string]any, totalSize)
		for k, v := range baseParams {
			newParams[k] = v
		}
		baseParams = newParams
	}

	// 合并所有额外的参数
	for _, m := range additionalParams {
		for k, v := range m {
			baseParams[k] = v
		}
	}

	return baseParams
}

func (s *SqlBuilder) commonQuery(bf bytes.Buffer, value map[string]any) (string, map[string]any) {
	if bf.Len() > 0 {
		bf.WriteString(" FROM ")
	}

	// 收集所有需要合并的 map
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

	// 使用统一的参数合并方法
	value = mergeParams(value, mapsToMerge...)

	return bf.String(), value
}

// Sql 统一获取 SQL 语句和参数
// 支持 SELECT、INSERT、UPDATE、DELETE 等所有操作
func (s *SqlBuilder) Sql() (string, map[string]any) {
	// 如果有 DML 操作类型，生成对应的 SQL
	switch s.dmlType {
	case "insert":
		return s.buildInsertMap()
	case "insert_many":
		return s.buildInsertMany()
	case "update":
		return s.buildUpdateMap()
	case "update_ordered":
		return s.buildUpdateOrdered()
	case "insert_on_duplicate_cols":
		return s.buildInsertOnDuplicateCols()
	case "insert_on_duplicate_map":
		return s.buildInsertOnDuplicateMap()
	case "delete":
		return s.buildDelete()
	default:
		// 默认是 SELECT 查询
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
}

// Delete 设置 DELETE 操作，返回 *SqlBuilder 以支持链式调用
// t 可选参数，指定要删除的表（用于多表 JOIN 删除的场景）
// 示例:
//   - Delete() -> DELETE FROM table WHERE ...
//   - Delete(&table) -> DELETE table_alias FROM table WHERE ...
//   使用: sql, params := builder.Table("user").Where(...).Delete().Sql()
func (s *SqlBuilder) Delete(t ...*SqlBuilder) *SqlBuilder {
	s.dmlType = "delete"
	if len(t) > 0 {
		s.deleteTarget = t[0]
	}
	return s
}

// buildDelete 构建 DELETE 查询语句
func (s *SqlBuilder) buildDelete() (string, map[string]any) {
	var value = map[string]any{}
	bf := bytes.Buffer{}
	sl := s.getDelete(s.deleteTarget)
	bf.WriteString(sl)
	return s.commonQuery(bf, value)
}
