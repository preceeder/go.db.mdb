package builder

// Field 接口表示 SQL 查询中的字段
type Field interface {
	Field() string         // 获取字段名
	String() string        // 获取字段的 SQL 字符串表示
	Values() *map[string]any // 获取字段关联的参数值
	As(string) Field       // 设置字段别名
}

// Expr 接口表示 SQL 查询中的表达式（条件、函数等）
type Expr interface {
	String() string        // 获取表达式的 SQL 字符串表示
	GetName() string       // 获取表达式关联的字段名（如果适用）
	Values() *map[string]any // 获取表达式关联的参数值
}

// Condition 实现 Expr 接口，表示 SQL 条件表达式
type Condition struct {
	Name  string           // 字段名（可选）
	S     string           // SQL 字符串
	Value *map[string]any  // 参数值
}

func (f Condition) String() string {
	return f.S
}

func (f Condition) GetName() string {
	return f.Name
}

func (f Condition) Values() *map[string]any {
	return f.Value
}
