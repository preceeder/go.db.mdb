package db

import (
	"context"
	"errors"
	"github.com/jmoiron/sqlx"
	"github.com/preceeder/db/builder"
	"os"
	"testing"
)

func newTestClient(t *testing.T) *MysqlClient {
	host := os.Getenv("MYSQL_HOST")
	port := os.Getenv("MYSQL_PORT")
	user := os.Getenv("MYSQL_USER")
	pass := os.Getenv("MYSQL_PASS")
	db := os.Getenv("MYSQL_DB")
	if host == "" || port == "" || user == "" || db == "" {
		t.Skip("skip: MYSQL_HOST/PORT/USER/PASS/DB not set")
	}
	cli := NewMysqlClient(MysqlConfig{
		Host:        host,
		Port:        port,
		User:        user,
		Password:    pass,
		Database:    db,
		MaxIdleCons: 2,
		MaxOpenCons: 5,
	})
	return cli
}

func TestQueryByBuilder_Simple(t *testing.T) {
	s := newTestClient(t)
	defer s.MysqlPoolClose()
	ctx := context.Background()

	tu := builder.Table("t_user")
	b := tu.Select(tu.Field("id")).First()
	var row struct {
		Id int64 `db:"id"`
	}
	_ = s.QueryByBuilder(ctx, b, &row)
}

func TestFetchByBuilder_List(t *testing.T) {
	s := newTestClient(t)
	defer s.MysqlPoolClose()
	ctx := context.Background()

	tu := builder.Table("t_user")
	b := tu.Select(tu.Field("id")).Limit(5)
	var rows []struct {
		Id int64 `db:"id"`
	}
	_ = s.FetchByBuilder(ctx, b, &rows)
}

// 可选：设置 MYSQL_TEST_DML=1 才会跑 DML 用例
func TestExecByBuilder_DML(t *testing.T) {
	if os.Getenv("MYSQL_TEST_DML") != "1" {
		t.Skip("skip: MYSQL_TEST_DML != 1")
	}
	s := newTestClient(t)
	defer s.MysqlPoolClose()
	ctx := context.Background()

	err := s.Transaction(ctx, func(ctx context.Context, m MysqlClient, tx *sqlx.Tx) error {
		// 使用临时表，避免污染
		_, _ = tx.Exec("CREATE TEMPORARY TABLE IF NOT EXISTS tmp_mdb_test (id BIGINT PRIMARY KEY AUTO_INCREMENT, name VARCHAR(64), age INT) ENGINE=InnoDB")

		// Insert
		insSQL, insParams := builder.Table("tmp_mdb_test").InsertMap(map[string]any{"name": "n1", "age": 18})
		if _, err := m.ExecByBuilder(ctx, insSQL, insParams, tx); err != nil {
			t.Fatalf("insert failed")
		}

		// Update
		tb := builder.Table("tmp_mdb_test")
		updSQL, updParams := tb.Where(tb.Field("name").Eq("n1", "n")).UpdateMap(map[string]any{"age": 19})
		if _, err := m.ExecByBuilder(ctx, updSQL, updParams, tx); err != nil {
			t.Fatalf("update failed")
		}

		// QueryByBuilder inside tx
		qb := tb.Select(tb.Field("id")).Where(tb.Field("name").Eq("n1", "n")).First()
		var row struct {
			Id int64 `db:"id"`
		}
		_ = m.QueryByBuilder(ctx, qb, &row, tx)

		// FetchByBuilder inside tx
		fb := tb.Select(tb.Field("id")).Limit(10)
		var list []struct {
			Id int64 `db:"id"`
		}
		_ = m.FetchByBuilder(ctx, fb, &list, tx)

		// Delete
		delSQL, delParams := tb.Where(tb.Field("age").Eq(19, "a")).Delete()
		if _, err := m.ExecByBuilder(ctx, delSQL, delParams, tx); err != nil {
			t.Fatalf("delete failed")
		}

		// 强制回滚
		return errors.New("rollback")
	})
	if err != nil && err.Error() != "rollback" {
		t.Fatalf("transaction failed: %v", err)
	}
}
