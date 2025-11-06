package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/preceeder/db/builder"
	"log/slog"
	"strings"
)

type MysqlClient struct {
	MysqlConfig MysqlConfig
	Db          *sqlx.DB
}

type MysqlConfig struct {
	Host        string `json:"host" yaml:"host"`
	Port        string `json:"port" yaml:"port"`
	Password    string `json:"password" yaml:"password"`
	User        string `json:"user" yaml:"user"`
	Database    string `json:"database" yaml:"database"`
	MaxOpenCons int    `json:"maxOpenCons" yaml:"maxOpenCons"`
	MaxIdleCons int    `json:"maxIdleCons" yaml:"maxIdleCons"`
	Params      string `json:"params" json:"params"` // 其他配置数据, 放在链接后面的参数重
}

func NewMysqlClient(config MysqlConfig) *MysqlClient {
	db := initMySQL(config)

	return &MysqlClient{
		Db:          db,
		MysqlConfig: config,
	}
}

// 初始化数据库
func initMySQL(config MysqlConfig) *sqlx.DB {

	//dsn := "root:password@tcp(127.0.0.1:3306)/database"
	dsn := fmt.Sprintf("%v:%v@tcp(%v:%v)/%v", config.User, config.Password, config.Host, config.Port, config.Database)
	if config.Params != "" {
		dsn = strings.Join([]string{dsn, config.Params}, "?")
	}
	slog.Info("链接数据库", "db", dsn)
	// 安全链接  内部已经ping 了
	db := sqlx.MustConnect("mysql", dsn)
	db.SetMaxOpenConns(config.MaxOpenCons)
	db.SetMaxIdleConns(config.MaxIdleCons)
	return db
}

func (s MysqlClient) MysqlPoolClose() {
	err := s.Db.Close()
	if err != nil {
		slog.Error("关闭数据库错误", "error", err.Error())
		return
	}
	slog.Info("close mdb", "config", s.MysqlConfig)
}

// 参数解析（安全版本）
// 返回 error，避免 panic，便于调用方控制错误处理
func (s MysqlClient) sqlParseSafe(ctx context.Context, osql string, params map[string]any) (string, []any, error) {
	q, args, err := sqlx.Named(osql, params)
	if err != nil {
		slog.ErrorContext(ctx, "sqlx.Named", "error", err.Error())
		return "", nil, err
	}
	q, args, err = sqlx.In(q, args...)
	if err != nil {
		slog.ErrorContext(ctx, "sqlx.In", "error", err.Error(), "params", params, "sql", q)
		return "", nil, err
	}
	q = s.Db.Rebind(q)
	return q, args, nil
}

// -------------------- Builder 集成 --------------------

// QueryByBuilder 执行由 builder 生成的单行查询
func (s MysqlClient) QueryByBuilder(ctx context.Context, b *builder.SqlBuilder, dest any, tx ...*sqlx.Tx) error {
	sqlStr, params := b.Query()
	q, args, err := s.sqlParseSafe(ctx, sqlStr, params)
	if err != nil {
		return err
	}
	if len(tx) > 0 && tx[0] != nil {
		err = tx[0].Get(dest, q, args...)
	} else {
		err = s.Db.Get(dest, q, args...)
	}
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return err
	case err != nil:
		slog.ErrorContext(ctx, "mdb QueryByBuilder failed", "error", err, "sql", sqlStr, "data", params)
		return err
	}
	return nil
}

// FetchByBuilder 执行由 builder 生成的多行查询
func (s MysqlClient) FetchByBuilder(ctx context.Context, b *builder.SqlBuilder, dest any, tx ...*sqlx.Tx) error {
	sqlStr, params := b.Query()
	q, args, err := s.sqlParseSafe(ctx, sqlStr, params)
	if err != nil {
		return err
	}
	if len(tx) > 0 && tx[0] != nil {
		if err = tx[0].Select(dest, q, args...); err != nil {
			slog.ErrorContext(ctx, "mdb FetchByBuilder failed", "error", err, "sql", sqlStr, "data", params)
			return err
		}
	} else if err = sqlx.Select(s.Db, dest, q, args...); err != nil {
		slog.ErrorContext(ctx, "mdb FetchByBuilder failed", "error", err, "sql", sqlStr, "data", params)
		return err
	}
	return nil
}

// ExecByBuilder 执行由 builder 生成的 DML 语句（Insert/Update/Delete）
func (s MysqlClient) ExecByBuilder(ctx context.Context, sqlStr string, params map[string]any, tx ...*sqlx.Tx) (sql.Result, error) {
	q, args, err := s.sqlParseSafe(ctx, sqlStr, params)
	if err != nil {
		return nil, err
	}
	var rs sql.Result
	if len(tx) > 0 && tx[0] != nil {
		rs, err = tx[0].Exec(q, args...)
	} else {
		rs, err = s.Db.Exec(q, args...)
	}
	if err != nil {
		slog.ErrorContext(ctx, "mdb ExecByBuilder failed", "error", err, "sql", q, "data", params)
		return nil, err
	}
	return rs, nil
}

// map[string]any{"tableName": "t_user",  "Set":map[string]any{"nick": "nihao"}, "Where":map[string]any{"userId": "1111"}}
// 下面你的跟新方法 可以按照户指定顺序更新字段,  有些时候需要指定更新顺序的 就用下买你的方法传入参数
// map[string]any{"tableName": "t_user",  "Set":[]map[string]any{{"nick": "nihao"}, {"name": []string{"if(s=0, 1, 0)"}}}, "Where":map[string]any{"userId": "1111"}}

func (s MysqlClient) Transaction(ctx context.Context, queryObj func(context.Context, MysqlClient, *sqlx.Tx) error) (err error) {
	beginx, err := s.Db.Beginx()

	if err != nil {
		slog.ErrorContext(ctx, "begin trans failed", "error", err.Error())
		return
	}
	defer func() {
		if p := recover(); p != nil {
			err = beginx.Rollback()
			slog.ErrorContext(ctx, "事务回滚", "error", err.Error())
			if err != nil {
				return
			}
		} else {
			err = beginx.Commit()
			if err != nil {
				slog.ErrorContext(ctx, "提交失败", "error", err)
				return
			}
		}
	}()
	if er := queryObj(ctx, s, beginx); er != nil {
		err = beginx.Rollback()
		slog.ErrorContext(ctx, "事务回滚", "error", er)
		if er != nil {
			return
		}

	}
	return
}
