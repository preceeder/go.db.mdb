package builder

import (
	"fmt"
	"testing"
)

// TestMinMaxWithField 测试 Min 和 Max 接受 Field 类型
func TestMinMaxWithField(t *testing.T) {
	t.Run("Min 接受 Field 类型", func(t *testing.T) {
		// 使用 Field 类型
		field := NewField("age")
		result := Min(field)
		
		sql := result.String()
		fmt.Println("Min(Field):", sql)
		if sql == "" {
			t.Error("Min() 应该返回有效的 SQL 字符串")
		}
	})

	t.Run("Max 接受 Field 类型", func(t *testing.T) {
		// 使用 Field 类型
		field := NewField("age")
		result := Max(field)
		
		sql := result.String()
		fmt.Println("Max(Field):", sql)
		if sql == "" {
			t.Error("Max() 应该返回有效的 SQL 字符串")
		}
	})

	t.Run("Min 接受字符串", func(t *testing.T) {
		// 使用字符串
		result := Min("age")
		
		sql := result.String()
		fmt.Println("Min(string):", sql)
		if sql == "" {
			t.Error("Min() 应该返回有效的 SQL 字符串")
		}
	})

	t.Run("Max 接受字符串", func(t *testing.T) {
		// 使用字符串
		result := Max("age")
		
		sql := result.String()
		fmt.Println("Max(string):", sql)
		if sql == "" {
			t.Error("Max() 应该返回有效的 SQL 字符串")
		}
	})

	t.Run("链式调用 Min/Max", func(t *testing.T) {
		field := NewField("age")
		result := field.Min().As("min_age")
		
		sql := result.String()
		fmt.Println("field.Min().As():", sql)
		
		result2 := field.Max().As("max_age")
		sql2 := result2.String()
		fmt.Println("field.Max().As():", sql2)
	})

	t.Run("在子查询中使用 Min/Max", func(t *testing.T) {
		// 子查询中使用 Field
		subQuery := Table("t_user_info").
			Select(
				NewField("user_id"),
				Min(NewField("age")).As("min_age"),
				Max(NewField("age")).As("max_age"),
			).
			Group(NewField("user_id")).
			Label("stats")

		// 主查询
		tu := Table("t_user").As("u")
		sql, params := tu.
			Select(
				tu.Field("id"),
				subQuery.Field("min_age"),
				subQuery.Field("max_age"),
			).
			LeftJoin(subQuery, tu.Field("id").Eq(subQuery.Field("user_id"))).
			Query()

		fmt.Println("=== 子查询中使用 Min/Max ===")
		fmt.Println("SQL:", sql)
		fmt.Println("参数:", params)
	})

	t.Run("Min/Max 配合其他函数", func(t *testing.T) {
		field := NewField("price")
		
		// Min 配合 Sum
		result1 := Min(Sum(field).As("total_price"))
		fmt.Println("Min(Sum(field)):", result1.String())
		
		// Max 配合 Count
		result2 := Max(Count(field).As("count"))
		fmt.Println("Max(Count(field)):", result2.String())
	})

	t.Run("Min/Max 在复杂查询中", func(t *testing.T) {
		tu := Table("t_order")
		sql, _ := tu.
			Select(
				tu.Field("user_id"),
				Min(tu.Field("price")).As("min_price"),
				Max(tu.Field("price")).As("max_price"),
				Sum(tu.Field("price")).As("total_price"),
			).
			Where(tu.Field("status").Eq(1)).
			Group(tu.Field("user_id")).
			Having(
				Max(tu.Field("price")).Gt(1000),
			).
			Query()

		fmt.Println("=== 复杂查询中的 Min/Max ===")
		fmt.Println("SQL:", sql)
	})
}

