package builder

import (
	"fmt"
	"strings"
	"testing"
)

func TestInsertMap(t *testing.T) {
	tbl := Table("t_user")
	sql, params := tbl.InsertMap(map[string]any{
		"name": "nick",
		"age":  23,
	}).Sql()
	fmt.Println("InsertMap SQL:", sql)
	fmt.Println("InsertMap Params:", params)
	if sql == "" || len(params) != 2 {
		t.Fatal("InsertMap build failed")
	}
	if want := "INSERT INTO t_user (`name`, `age`) VALUES (:name, :age)"; sql != want {
		// 仅校验基本格式，不严格要求列顺序（map 无序），这里不强校验
		fmt.Println("Note: column order may vary due to map iteration")
	}
}

func TestInsertMany(t *testing.T) {
	tbl := Table("t_user")
	rows := []map[string]any{
		{"name": "a", "age": 1},
		{"name": "b", "age": 2},
	}
	sql, params := tbl.InsertMany(rows).Sql()
	fmt.Println("InsertMany SQL:", sql)
	fmt.Println("InsertMany Params:", params)
	if sql == "" || len(params) != 4 { // 2 rows * 2 cols
		t.Fatal("InsertMany build failed")
	}
	if _, ok := params["name_0"]; !ok {
		t.Fatal("InsertMany param keys not expanded as expected")
	}
}

func TestUpdateMap_WithWhere(t *testing.T) {
	tbl := Table("t_user").As("u")
	sql, params := tbl.
		Where(tbl.Field("id").Eq(1, "id")).
		UpdateMap(map[string]any{
			"name": "new",
			"age":  30,
		}).Sql()
	fmt.Println("UpdateMap SQL:", sql)
	fmt.Println("UpdateMap Params:", params)
	if sql == "" || len(params) != 3 { // name, age, id
		t.Fatal("UpdateMap build failed")
	}
	if _, ok := params["id"]; !ok {
		t.Fatal("UpdateMap missing where param")
	}
}

func TestUpdateMap_WithJoin(t *testing.T) {
	u := Table("t_user").As("u")
	info := Table("t_user_info").As("i")
	sql, params := u.LeftJoin(info, u.Field("id").Eq(info.Field("user_id"))).
		Where(info.Field("user_id").Eq(2, "uid")).
		UpdateMap(map[string]any{
			"name": "mike",
		}).Sql()
	fmt.Println("UpdateMap (join) SQL:", sql)
	fmt.Println("UpdateMap (join) Params:", params)
	if sql == "" || len(params) != 2 { // name, uid
		t.Fatal("UpdateMap with join build failed")
	}
}

func TestDelete_WithWhere(t *testing.T) {
	tbl := Table("t_user").As("u")
	sql, params := tbl.
		Where(tbl.Field("id").Eq(1, "id")).
		Delete().Sql()
	fmt.Println("Delete SQL:", sql)
	fmt.Println("Delete Params:", params)
	if sql == "" || len(params) != 1 {
		t.Fatal("Delete build failed")
	}
}

func TestDelete_WithJoinTarget(t *testing.T) {
	u := Table("t_user").As("u")
	i := Table("t_user_info").As("i")
	sql, params := u.LeftJoin(i, u.Field("id").Eq(i.Field("user_id"))).
		Where(i.Field("user_id").IsNull(), u.Field("status").Eq(0, "st")).
		Delete(u).Sql()
	fmt.Println("Delete (join target) SQL:", sql)
	fmt.Println("Delete (join target) Params:", params)
	if sql == "" || len(params) != 1 {
		t.Fatal("Delete with join target build failed")
	}
}

func TestDelete_WithoutWhere(t *testing.T) {
	tbl := Table("t_tmp_clean")
	sql, params := tbl.Delete().Sql()
	fmt.Println("Delete no where SQL:", sql)
	fmt.Println("Delete no where Params:", params)
	if sql == "" {
		t.Fatal("Delete no where build failed")
	}
}

func TestInsertIgnoreMap(t *testing.T) {
	tbl := Table("t_user")
	sql, params := tbl.InsertIgnoreMap(map[string]any{
		"name": "nick",
		"age":  23,
	}).Sql()
	fmt.Println("InsertIgnoreMap SQL:", sql)
	fmt.Println("InsertIgnoreMap Params:", params)
	if sql == "" || len(params) != 2 {
		t.Fatal("InsertIgnoreMap build failed")
	}
	if want := "INSERT IGNORE INTO t_user (`name`, `age`) VALUES (:name, :age)"; sql != want {
		fmt.Println("Note: column order may vary due to map iteration")
	}
}

func TestInsertIgnoreMany(t *testing.T) {
	tbl := Table("t_user")
	rows := []map[string]any{
		{"name": "a", "age": 1},
		{"name": "b", "age": 2},
	}
	sql, params := tbl.InsertIgnoreMany(rows).Sql()
	fmt.Println("InsertIgnoreMany SQL:", sql)
	fmt.Println("InsertIgnoreMany Params:", params)
	if sql == "" || len(params) != 4 {
		t.Fatal("InsertIgnoreMany build failed")
	}
}

func TestInsertOnDuplicateColsMany(t *testing.T) {
	rows := []map[string]any{
		{"name": "a", "age": 1},
		{"name": "b", "age": 2},
	}
	tbl := Table("t_user")
	sql, params := tbl.InsertOnDuplicateColsMany(rows, []string{"name"}).Sql()
	fmt.Println("InsertOnDuplicateColsMany SQL:", sql)
	fmt.Println("InsertOnDuplicateColsMany Params:", params)
	if sql == "" || len(params) != 4 {
		t.Fatal("InsertOnDuplicateColsMany build failed")
	}
}

func TestInsertOnDuplicateMapMany(t *testing.T) {
	rows := []map[string]any{
		{"name": "a", "age": 1},
		{"name": "b", "age": 2},
	}
	update := map[string]any{
		"age": 18,
	}
	tbl := Table("t_user")
	sql, params := tbl.InsertOnDuplicateMapMany(rows, update).Sql()
	fmt.Println("InsertOnDuplicateMapMany SQL:", sql)
	fmt.Println("InsertOnDuplicateMapMany Params:", params)
	if sql == "" || len(params) != 4+len(update) {
		t.Fatal("InsertOnDuplicateMapMany build failed")
	}
}

func TestInsertOnDuplicateMapMany_WithExpressions(t *testing.T) {
	rows := []map[string]any{
		{
			"username":         "name",
			"func_code":        "sd",
			"rule_id":          "ed",
			"available_quota":  "se",
			"reset_cycle_days": "sde",
			"last_reset_time":  "sdes",
			"next_reset_time":  "ss",
			"expire_time":      "2025-01-03",
		},
	}
	update := map[string]any{
		"expire_time": SetExpr("DATE_ADD(`expire_time`, INTERVAL 30 DAY)", nil),
	}

	tbl := Table("user_quota_account")
	sqlStr, params := tbl.InsertOnDuplicateMapMany(rows, update).Sql()

	if sqlStr == "" {
		t.Fatal("InsertOnDuplicateMapMany with expression build failed")
	}
	if !strings.Contains(sqlStr, "`expire_time` = DATE_ADD(`expire_time`, INTERVAL 30 DAY)") {
		t.Fatalf("InsertOnDuplicateMapMany missing SetExpr expression, got: %s", sqlStr)
	}

	// 应包含 1 行插入参数
	if len(params) != len(rows[0]) {
		t.Fatalf("InsertOnDuplicateMapMany params unexpected: %+v", params)
	}
}

func TestUpdateMap_WithExpressions(t *testing.T) {
	tbl := Table("t_user").As("u")
	sql, params := tbl.
		Where(tbl.Field("id").Eq(1, "id")).
		UpdateMap(map[string]any{
			"amount": tbl.Field("amount").Sub(2),
			"score":  SetExpr("COALESCE(`score`, 0) + :score_inc", map[string]any{"score_inc": 5}),
		}).Sql()

	if sql == "" {
		t.Fatal("UpdateMap with expressions build failed")
	}

	if !strings.Contains(sql, "`amount` = u.amount - 2") {
		t.Fatalf("UpdateMap should include field arithmetic, got: %s", sql)
	}
	if !strings.Contains(sql, "`score` = COALESCE(`score`, 0) + :score_inc") {
		t.Fatalf("UpdateMap should include SetExpr expression, got: %s", sql)
	}
	if params["score_inc"] != 5 {
		t.Fatalf("UpdateMap parameters missing or incorrect: %+v", params)
	}
	if params["id"] != 1 {
		t.Fatalf("UpdateMap should keep where params, got: %+v", params)
	}
}

func TestUpdateOrdered_WithExpressions(t *testing.T) {
	tbl := Table("t_user").As("u")
	sql, params := tbl.
		Where(tbl.Field("id").Eq(1, "id")).
		UpdateOrdered([]map[string]any{
			{"amount": tbl.Field("amount").Sub(2)},
			{"score": SetExpr("COALESCE(`score`, 0) + :score_inc", map[string]any{"score_inc": 5})},
		}).Sql()

	if sql == "" {
		t.Fatal("UpdateOrdered with expressions build failed")
	}

	idxAmount := strings.Index(sql, "`amount` = u.amount - 2")
	idxScore := strings.Index(sql, "`score` = COALESCE(`score`, 0) + :score_inc")
	if idxAmount == -1 || idxScore == -1 {
		t.Fatalf("UpdateOrdered missing expected expressions, got: %s", sql)
	}
	if idxAmount > idxScore {
		t.Fatalf("UpdateOrdered should preserve custom order, got: %s", sql)
	}
	if params["score_inc"] != 5 {
		t.Fatalf("UpdateOrdered parameters missing or incorrect: %+v", params)
	}
	if params["id"] != 1 {
		t.Fatalf("UpdateOrdered should keep where params, got: %+v", params)
	}
}
