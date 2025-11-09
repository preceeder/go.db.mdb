package builder

import (
	"testing"
)

// BenchmarkSimpleQuery 测试简单查询性能
func BenchmarkSimpleQuery(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tu := Table("t_user")
		sql := tu.Select(tu.Field("id"), tu.Field("name")).
			Where(tu.Field("id").Eq(1)).
			Limit(10)
		_, _ = sql.Sql()
	}
}

// BenchmarkComplexQuery 测试复杂查询性能
func BenchmarkComplexQuery(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tu := Table("t_user").As("u")
		ti := Table("t_user_info").As("t")
		
		sql := tu.Select(tu.Field("id"), tu.Field("name"), ti.Field("age")).
			LeftJoin(ti, tu.Field("id").Eq(ti.Field("user_id"))).
			Where(
				tu.Field("id").Gt(0),
				Or(
					And(tu.Field("name").Eq("test"), tu.Field("age").Gte(18)),
					tu.Field("status").Eq(1),
				),
			).
			Group(tu.Field("status")).
			Having(Sum(tu.Field("age")).Gt(100)).
			Order(tu.Field("id").Desc()).
			Limit(10).
			Offset(5)
		_, _ = sql.Sql()
	}
}

// BenchmarkFieldOperations 测试字段操作性能
func BenchmarkFieldOperations(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		field := NewField("age")
		_ = field.Add(10).Mul(2).Div(3).Count().Max().As("calculated_age")
	}
}

// BenchmarkStringSliceToString 测试字符串数组转换性能
func BenchmarkStringSliceToString(b *testing.B) {
	ss := []string{"test1", "test2", "test3", "test4", "test5"}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = StringSliceToString(ss)
	}
}

// BenchmarkNumberSliceToString 测试数字数组转换性能
func BenchmarkNumberSliceToString(b *testing.B) {
	nums := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = NumberSliceToString(nums)
	}
}

// BenchmarkColumnNameHandler 测试列名处理性能
func BenchmarkColumnNameHandler(b *testing.B) {
	fields := []string{"id", "user_name", "create_time", "table.field", "`already_quoted`"}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, f := range fields {
			_ = ColumnNameHandler(f)
		}
	}
}

// BenchmarkConditionBuilding 测试条件构建性能
func BenchmarkConditionBuilding(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		field := NewField("age")
		_ = And(
			field.Eq(18),
			field.Gt(10),
			field.Lt(100),
			field.In([]int{1, 2, 3, 4, 5}),
		)
	}
}

// BenchmarkMapMerge 测试 map 合并性能（模拟参数合并）
func BenchmarkMapMerge(b *testing.B) {
	map1 := map[string]any{"a": 1, "b": 2, "c": 3}
	map2 := map[string]any{"d": 4, "e": 5}
	map3 := map[string]any{"f": 6, "g": 7}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result := make(map[string]any)
		for k, v := range map1 {
			result[k] = v
		}
		for k, v := range map2 {
			result[k] = v
		}
		for k, v := range map3 {
			result[k] = v
		}
		_ = result
	}
}

