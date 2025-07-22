package builder

func Table(tbl string) *SqlBuilder {
	s := SqlBuilder{}
	s.Table = &table{
		Name: ColumnNameHandler(tbl),
	}
	return &s
}
