package builder

type Field interface {
	Field() string
	String() string
	Values() *map[string]any
	As(string) Field // 设置别名
}
type Expr interface { // 表达式
	String() string
	GetName() string
	Values() *map[string]any
}

type Condition struct {
	Name  string
	S     string
	Value *map[string]any
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
