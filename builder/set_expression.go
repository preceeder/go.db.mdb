package builder

// SetExpression 表示 UPDATE 语句中 SET 子句右侧的自定义表达式。
// 通过它可以在保持参数化的同时，使用字段或函数构造更灵活的更新语句，
// 例如：SetExpression("`amount` - 2", nil) -> `amount` = `amount` - 2
type SetExpression struct {
	expr   string
	params map[string]any
}

// SetExpr 创建一个 SetExpression。
// expr 需要提供完整的右侧表达式，params 用于传入该表达式中使用的参数（可选）。
func SetExpr(expr string, params map[string]any) SetExpression {
	if params == nil {
		params = map[string]any{}
	}
	return SetExpression{
		expr:   expr,
		params: params,
	}
}

func (s SetExpression) expression() string {
	return s.expr
}

func (s SetExpression) parameters() map[string]any {
	return s.params
}

