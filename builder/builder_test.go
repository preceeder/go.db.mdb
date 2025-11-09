package builder

import (
	"fmt"
	"testing"
)

func TestTable(t *testing.T) {
	//testUnion()
	//testDelete()

}

func TestJoin(b *testing.T) {
	tu := Table("t_user").As("u")
	ti := Table("t_user_info").As("t")
	// 子查询
	ti = ti.Select(ti.Field("user_id")).Where(ti.Field("user_name").Eq("不是吧")).Label("hah")

	tl := Table("t_user_login_info")

	sql := tu.Select(tu.Field("id"), tu.Field("name"), ti.Field("age"), tl.Field("channel")).
		LeftJoin(ti, tu.Field("id").Eq(ti.Field("user_id"))).
		LeftJoin(tl, tu.Field("id").Eq(tl.Field("user_id"))).
		Where(tu.Field("id").Gt(0),
			Or(And(tu.Field("sexual").Eq("男"), tu.Field("create_time").Gt(UnixTimeStamp(tl.Field("create_time")))),
				tu.Field("sexual").Eq("女")),
		).Group(tu.Field("sexual"), tu.Field("age")).
		Having(Sum(tu.Field("age")).Gt(20)).
		Limit(5).OffSet(2)
	sqlStr, data := sql.Sql()
	fmt.Println(sqlStr)
	fmt.Println(data)
}

func testUnion() {
	tu := Table("t_user").Select("id", "name", "age")
	tu = tu.Where(tu.Field("id").Gt(0), tu.Field("name").Eq("<UNK>"))
	ti := Table("t_user_info").Select("id", "name", "age")
	ti = ti.Where(ti.Field("id").Gt(23), ti.Field("name").Eq("<UNoiu"))
	tl := Table("t_user_login_info").Select("id", "name", "age")
	tl = tl.Where(tl.Field("id").Gt(23), tl.Field("name").Eq("<UNoiu"))
	// 需要定义一个空表 全部转换成方  select oiu.id from ((table1) union (table2) union (table3)) oiu
	uni := Table("").Label("oiu")
	uni = uni.Union(tu).Union(tl).Union(ti).Select(uni.Field("id"), uni.Field("name")).Group(uni.Field("id")).Order().Limit(5)
	uni = uni.Order(uni.Field("id").Desc(), uni.Field("name").Asc())
	sqlStr, data := uni.Sql()
	fmt.Println(sqlStr)
	fmt.Println(data)
}

func testDelete() {
	tu := Table("t_user").As("u")
	ti := Table("t_user_info").As("t")

	fr, data := tu.Where(tu.Field("id").Eq(34), tu.Field("name").Eq("nus")).Delete().Sql()
	fmt.Println(fr)
	fmt.Println(data)

	de, data := tu.LeftJoin(ti, tu.Field("id").Eq(ti.Field("id"))).
		Where(ti.Field("user_id").IsNull(), tu.Field("user_name").Eq("<UNK>")).
		Delete(tu).Sql()
	fmt.Println(de)
	fmt.Println(data)

	tt := ti.Select(ti.Field("id")).Where(ti.Field("user_id").Gt(34)).Label("sd")
	de, data = tu.LeftJoin(tt, tu.Field("id").Eq(tt.Field("id"))).
		Where(tt.Field("user_id").IsNull(), tu.Field("user_name").Eq("<UNK>")).
		Delete(tu).Sql()
	fmt.Println(de, data)
}

// BenchmarkSimpleQuery 测试简单查询性能
func TestSimpleQuery(t *testing.T) {
	tu := Table("t_user")
	sql := tu.Select(tu.Field("id"), tu.Field("name")).
		Where(tu.Field("id").Eq(1)).
		Limit(10).Offset(20)
	sql2, data := sql.Sql()

	fmt.Println(sql2)
	fmt.Println(data)
}

// BenchmarkComplexQuery 测试复杂查询性能
func TestComplexQuery(b *testing.T) {
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
	sql2, data := sql.Sql()
	fmt.Println(sql2)
	fmt.Println(data)

}
