package builder

import (
	"fmt"
	"testing"
)

func TestTable(t *testing.T) {
	//testJoin()
	//testUnion()
	//testDelete()

	testInsert()
}

func testJoin() {
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
	sqlStr, data := sql.Query()
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
	sqlStr, data := uni.Query()
	fmt.Println(sqlStr)
	fmt.Println(data)
}

func testDelete() {
	tu := Table("t_user").As("u")
	ti := Table("t_user_info").As("t")

	fr, data := tu.Where(tu.Field("id").Eq(34), tu.Field("name").Eq("nus")).Delete()
	fmt.Println(fr)
	fmt.Println(data)

	de, data := tu.LeftJoin(ti, tu.Field("id").Eq(ti.Field("id"))).
		Where(ti.Field("user_id").IsNull(), tu.Field("user_name").Eq("<UNK>")).
		Delete(tu)
	fmt.Println(de)
	fmt.Println(data)

	tt := ti.Select(ti.Field("id")).Where(ti.Field("user_id").Gt(34)).Label("sd")
	de, data = tu.LeftJoin(tt, tu.Field("id").Eq(tt.Field("id"))).
		Where(tt.Field("user_id").IsNull(), tu.Field("user_name").Eq("<UNK>")).
		Delete(tu)
	fmt.Println(de, data)
}

func testInsert() {
	tu := Table("t_user")
	name := []string{"sd", "dr"}
	tu.Insert(tu.Field("name").In(name), tu.Field("age").Eq(23))
}
