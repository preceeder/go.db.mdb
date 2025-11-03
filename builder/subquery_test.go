package builder

import (
	"fmt"
	"testing"
)

// TestSubqueryInJoin 测试在 JOIN 中使用子查询
func TestSubqueryInJoin(t *testing.T) {
	t.Run("子查询作为 JOIN 表", func(t *testing.T) {
		// 主表
		tu := Table("t_user").As("u")

		// 子查询：查询用户信息表
		ti := Table("t_user_info").
			Select("user_id", "age", "address").
			Where(NewField("age").Gt(18)).
			Label("info") // 设置子查询别名，必须在最后调用

		// 主查询：JOIN 子查询
		sql, params := tu.
			Select(tu.Field("id"), tu.Field("name"), ti.Field("age"), ti.Field("address")).
			LeftJoin(ti, tu.Field("id").Eq(ti.Field("user_id"))).
			Where(tu.Field("id").Gt(0)).
			Query()

		fmt.Println("=== 子查询 JOIN 测试 ===")
		fmt.Println("SQL:", sql)
		fmt.Println("参数:", params)
		fmt.Println()
	})

	t.Run("多层嵌套子查询 JOIN", func(t *testing.T) {
		tu := Table("t_user").As("u")

		// 第一层子查询
		ti := Table("t_user_info").
			Select("user_id", "age").
			Where(NewField("age").Gt(18)).
			Label("info1")

		// 第二层子查询（嵌套在第一层中）
		tl := Table("t_user_login_info").
			Select("user_id", "last_login_time").
			Where(NewField("last_login_time").Gt(UnixTimeStamp(NewField("2024-01-01")))).
			Label("login")

		// 主查询
		sql, params := tu.
			Select(tu.Field("id"), ti.Field("age"), tl.Field("last_login_time")).
			LeftJoin(ti, tu.Field("id").Eq(ti.Field("user_id"))).
			LeftJoin(tl, tu.Field("id").Eq(tl.Field("user_id"))).
			Query()

		fmt.Println("=== 多层嵌套子查询 JOIN 测试 ===")
		fmt.Println("SQL:", sql)
		fmt.Println("参数:", params)
		fmt.Println()
	})
}

// TestSubqueryInWhere 测试在 WHERE 条件中使用子查询
func TestSubqueryInWhere(t *testing.T) {
	t.Run("IN 子查询", func(t *testing.T) {
		// 子查询：获取活跃用户 ID 列表
		subSQL, _ := Table("t_user_login_info").
			Select("user_id").
			Where(NewField("last_login_time").Gt(UnixTimeStamp(NewField("2024-01-01")))).
			Query()

		// 主查询：查询这些活跃用户
		// 注意：In 方法可以直接接受 SQL 字符串作为子查询
		tu := Table("t_user")
		sql, params := tu.
			Select("*").
			Where(tu.Field("id").In(fmt.Sprintf("(%s)", subSQL))). // 使用子查询结果，需要用括号包裹
			Query()

		fmt.Println("=== IN 子查询测试 ===")
		fmt.Println("SQL:", sql)
		fmt.Println("参数:", params)
		fmt.Println()
	})

	t.Run("EXISTS 子查询", func(t *testing.T) {
		tu := Table("t_user").As("u")

		// EXISTS 子查询
		subSQL, _ := Table("t_user_info").
			Select("1"). // EXISTS 只需要检查存在性
			Where(NewField("user_id").Eq(NewField("u.id"))).
			Query()

		// 主查询
		sql, params := tu.
			Select("*").
			Where(NotExists(subSQL)). // NOT EXISTS
			Query()

		fmt.Println("=== EXISTS 子查询测试 ===")
		fmt.Println("SQL:", sql)
		fmt.Println("参数:", params)
		fmt.Println()
	})

	t.Run("比较运算符子查询", func(t *testing.T) {
		tu := Table("t_user")

		// 子查询：获取平均年龄
		subSQL, _ := Table("t_user_info").
			Select(Sum(NewField("age")).As("avg_age")).
			Query()

		// 主查询：年龄大于平均年龄的用户
		// 使用字符串拼接子查询到字段中
		avgAgeField := NewField(fmt.Sprintf("(%s)", subSQL))
		sql, params := tu.
			Select("*").
			Where(tu.Field("age").Gt(avgAgeField)).
			Query()

		fmt.Println("=== 比较运算符子查询测试 ===")
		fmt.Println("SQL:", sql)
		fmt.Println("参数:", params)
		fmt.Println()
	})
}

// TestSubqueryInFrom 测试在 FROM 子句中使用子查询
func TestSubqueryInFrom(t *testing.T) {
	t.Run("FROM 子查询", func(t *testing.T) {
		// 子查询：统计每个年龄段的用户数
		// 注意：这里实际上是通过 Label 创建子查询别名，然后在主查询中使用
		// 但 builder 的实现中，FROM 子查询需要特殊处理
		subQuery := Table("t_user").
			Select(
				NewField("age"),
				Count(NewField("id")).As("user_count"),
			).
			Group(NewField("age")).
			Label("age_stats") // 设置子查询别名

        // 使用 FromSub 将子查询作为 FROM 来源
        parent := Table("").FromSub(subQuery)
        sql, params := parent.
            Select(parent.Field("age"), parent.Field("user_count")).
            Where(parent.Field("user_count").Gt(10)).
            Order(parent.Field("age").Desc()).
            Query()

		fmt.Println("=== FROM 子查询测试 ===")
		fmt.Println("SQL:", sql)
		fmt.Println("参数:", params)
		fmt.Println()
	})
}

// TestSubqueryComplex 测试复杂子查询场景
func TestSubqueryComplex(t *testing.T) {
	t.Run("复杂嵌套子查询", func(t *testing.T) {
		// 第一层子查询：用户登录信息统计
		loginStats := Table("t_user_login_info").
			Select(
				NewField("user_id"),
				Count(NewField("id")).As("login_count"),
				Max(NewField("last_login_time")).As("last_login"),
			).
			Group(NewField("user_id")).
			Having(Count(NewField("id")).Gt(5)).
			Label("login_stats")

		// 第二层子查询：用户基本信息
		userInfo := Table("t_user").
			Select(
				NewField("id"),
				NewField("name"),
				NewField("age"),
			).
			Where(NewField("status").Eq(1)).
			Label("u")

        // 主查询：以 userInfo 作为 FROM 子查询，再 JOIN loginStats
        parent := Table("").FromSub(userInfo)
        sql, params := parent.
            Select(
                parent.Field("id"),
                parent.Field("name"),
                loginStats.Field("login_count"),
                loginStats.Field("last_login"),
            ).
            LeftJoin(loginStats, parent.Field("id").Eq(loginStats.Field("user_id"))).
            Where(
                parent.Field("age").Gte(18),
                Or(
                    parent.Field("name").Like("张%"),
                    parent.Field("name").Like("李%"),
                ),
            ).
            Order(loginStats.Field("login_count").Desc()).
            Limit(20).
            Query()

		fmt.Println("=== 复杂嵌套子查询测试 ===")
		fmt.Println("SQL:", sql)
		fmt.Println("参数:", params)
		fmt.Println()
	})

	t.Run("子查询配合聚合函数", func(t *testing.T) {
		tu := Table("t_user").As("u")

		// 子查询：每个用户的订单总数
		orderCount := Table("t_order").
			Select(
				NewField("user_id"),
				Count(NewField("id")).As("order_count"),
			).
			Group(NewField("user_id")).
			Label("oc")

		// 主查询
		sql, params := tu.
			Select(
				tu.Field("id"),
				tu.Field("name"),
				orderCount.Field("order_count"),
			).
			LeftJoin(orderCount, tu.Field("id").Eq(orderCount.Field("user_id"))).
			Where(tu.Field("id").In("SELECT user_id FROM t_order WHERE status = 1")).
			Query()

		fmt.Println("=== 子查询配合聚合函数测试 ===")
		fmt.Println("SQL:", sql)
		fmt.Println("参数:", params)
		fmt.Println()
	})

	t.Run("多表子查询 UNION", func(t *testing.T) {
		// 子查询 1
		t1 := Table("t_user").
			Select("id", "name", "create_time").
			Where(NewField("status").Eq(1))

		// 子查询 2
		t2 := Table("t_user_archive").
			Select("id", "name", "create_time").
			Where(NewField("status").Eq(1))

		// UNION 合并，Label 在最后
		uni := Table("").
			Union(t1).
			Union(t2).
			Label("merged")

		// 主查询
		sql, params := uni.
			Select(uni.Field("id"), uni.Field("name")).
			Where(uni.Field("create_time").Gt(UnixTimeStamp(NewField("2024-01-01")))).
			Order(uni.Field("create_time").Desc()).
			Query()

		fmt.Println("=== UNION 子查询测试 ===")
		fmt.Println("SQL:", sql)
		fmt.Println("参数:", params)
		fmt.Println()
	})
}

// TestSubqueryWithParams 测试带参数的子查询
func TestSubqueryWithParams(t *testing.T) {
	t.Run("子查询参数传递", func(t *testing.T) {
		// 子查询：根据传入的最小年龄筛选用户
		minAge := 18
		subQuery := Table("t_user_info").
			Select("user_id", "age").
			Where(NewField("age").Gte(minAge)).
			Label("info")

		// 主查询
		tu := Table("t_user")
		sql, params := tu.
			Select(tu.Field("id"), subQuery.Field("age")).
			LeftJoin(subQuery, tu.Field("id").Eq(subQuery.Field("user_id"))).
			Where(tu.Field("id").Gt(0)).
			Query()

		fmt.Println("=== 子查询参数传递测试 ===")
		fmt.Println("SQL:", sql)
		fmt.Println("参数:", params)
		fmt.Println()
	})
}

// TestSubqueryExamples 提供一些实用的子查询示例
func TestSubqueryExamples(t *testing.T) {
	fmt.Println("\n========== 子查询实用示例 ==========")

	// 示例 1: 查找没有订单的用户
	fmt.Println("示例 1: 查找没有订单的用户")
	tu := Table("t_user")
	subQuery, _ := Table("t_order").
		Select("user_id").
		Query()
	sql, _ := tu.
		Select("*").
		Where(tu.Field("id").NotIn(fmt.Sprintf("(%s)", subQuery))).
		Query()
	fmt.Println("SQL:", sql)
	fmt.Println()

	// 示例 2: 查找订单数最多的用户
	fmt.Println("示例 2: 查找订单数最多的用户")
	orderStats := Table("t_order").
		Select(
			NewField("user_id"),
			Count(NewField("id")).As("order_count"),
		).
		Group(NewField("user_id")).
		Order(Count(NewField("id")).Desc()).
		Limit(1).
		Label("top_user")
	sql, _ = orderStats.Query()
	fmt.Println("SQL:", sql)
	fmt.Println()

	// 示例 3: 相关子查询 - 每个用户的最后登录时间
	fmt.Println("示例 3: 相关子查询")
	loginQuery, _ := Table("t_user_login_info").
		Select(Max(NewField("login_time")).As("last_login")).
		Where(NewField("user_id").Eq(NewField("u.id"))).
		Query()
	tu = Table("t_user").As("u")
	sql, _ = tu.
		Select(
			tu.Field("id"),
			tu.Field("name"),
			NewField(fmt.Sprintf("(%s)", loginQuery)).As("last_login"),
		).
		Query()
	fmt.Println("SQL:", sql)
	fmt.Println()

	// 示例 4: 子查询作为计算字段
	fmt.Println("示例 4: 子查询作为计算字段")
	tu = Table("t_user").As("u")
	orderCountSub, _ := Table("t_order").
		Select(Count(NewField("id"))).
		Where(NewField("user_id").Eq(NewField("u.id"))).
		Query()
	sql, _ = tu.
		Select(
			tu.Field("id"),
			tu.Field("name"),
			NewField(fmt.Sprintf("(%s)", orderCountSub)).As("order_count"),
		).
		Query()
	fmt.Println("SQL:", sql)
	fmt.Println()
}

// TestSubqueryBestPractices 子查询最佳实践示例
func TestSubqueryBestPractices(t *testing.T) {
	fmt.Println("\n========== 子查询最佳实践 ==========")

	// 实践 1: Label() 必须在最后调用
	//fmt.Println("实践 1: 正确使用 Label()")
	//correct := Table("t_user").
	//	Select("id", "name").
	//	Where(NewField("status").Eq(1)).
	//	Label("u") // ✅ 正确：Label 在最后
	//
	//fmt.Println("正确的用法：在链式调用的最后调用 Label()")

	// 实践 2: 子查询字段访问
	fmt.Println("\n实践 2: 访问子查询字段")
	subQuery := Table("t_user_info").
		Select("user_id", "age").
		Label("info")

	tu := Table("t_user").As("u")
	sql, _ := tu.
		Select(
			tu.Field("id"),        // 主表字段
			subQuery.Field("age"), // ✅ 子查询字段
		).
		LeftJoin(subQuery, tu.Field("id").Eq(subQuery.Field("user_id"))).
		Query()
	fmt.Println("SQL:", sql)
	fmt.Println()
}
