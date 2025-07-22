package builder

import (
	"fmt"
	"testing"
)

func TestTable(t *testing.T) {
	//testJoin()
	testUnion()
}

func testJoin() {
	tu := Table("t_user")
	ti := Table("t_user_info")
	tl := Table("t_user_login_info")
	sql := tu.Select(tu.Field("id"), tu.Field("name"), ti.Field("age"), tl.Field("channel")).
		LeftJoin(ti, tu.Field("id").Eq(ti.Field("user_id"))).
		LeftJoin(tl, tu.Field("id").Eq(tu.Field("user_id"))).
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
	ti := Table("t_user_info").Select("id", "name", "ages")
	ti = ti.Where(ti.Field("id").Gt(23), ti.Field("name").Eq("<UNoiu"))
	tl := Table("t_user_login_info").Select("id", "name", "oiu")
	tl = tl.Where(tl.Field("id").Gt(23), tl.Field("name").Eq("<UNoiu"))
	// 需要定义一个空表 全部转换成方  select oiu.id from ((table1) union (table2) union (table3)) oiu
	uni := Table("").As("oiu")
	uni = uni.Union(tu).Union(tl).Union(ti).Select(uni.Field("id")).Group(uni.Field("id")).Order().Limit(5)
	uni = uni.Order(uni.Field("id").Desc(), uni.Field("name").Asc())
	sqlStr, data := uni.Query()
	fmt.Println(sqlStr)
	fmt.Println(data)
}
