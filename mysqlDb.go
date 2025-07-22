package mdb

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/preceeder/go/base"
	"log/slog"
	"reflect"
	"strings"
)

type MysqlClient struct {
	MysqlConfig MysqlConfig
	Db          *sqlx.DB
}

type MysqlConfig struct {
	Host        string `json:"host"`
	Port        string `json:"port"`
	Password    string `json:"password"`
	User        string `json:"user"`
	Db          string `json:"db"`
	MaxOpenCons int    `json:"maxOpenCons"`
	MaxIdleCons int    `json:"MaxIdleCons"`
	Params      string `json:"params"` // 其他配置数据, 放在链接后面的参数重
}

func NewMysqlClient(config MysqlConfig) MysqlClient {
	db := initMySQL(config)

	return MysqlClient{
		Db:          db,
		MysqlConfig: config,
	}
}

// 初始化数据库
func initMySQL(config MysqlConfig) *sqlx.DB {

	//dsn := "root:password@tcp(127.0.0.1:3306)/database"
	dsn := fmt.Sprintf("%v:%v@tcp(%v:%v)/%v", config.User, config.Password, config.Host, config.Port, config.Db)
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

func (s MysqlClient) getTableName(ctx base.BaseContext, params map[string]any) (tableName string) {
	if tn, ok := params["tableName"]; ok {
		tableName = tn.(string)
		delete(params, "tableName")
	} else {
		slog.ErrorContext(ctx, "mdb not table name", "params", params)
		panic("not table name")
	}
	return
}

// 参数解析
func (s MysqlClient) sqlPares(ctx base.BaseContext, osql string, params map[string]any) (sql string, args []any) {
	var err error
	sql, args, err = sqlx.Named(osql, params)
	if err != nil {
		slog.ErrorContext(ctx, "sqlx.Named", "error", err.Error())
		panic(errors.New("sqlx.Named error :" + err.Error()))
	}
	sql, args, err = sqlx.In(sql, args...)
	if err != nil {
		slog.ErrorContext(ctx, "sqlx.In", "error", err.Error(), "params", params, "sql", sql)
		panic(errors.New("sqlx.In error :" + err.Error()))
	}
	sql = s.Db.Rebind(sql)
	return sql, args
}

func (s MysqlClient) Select(ctx base.BaseContext, sqlStr string, params map[string]any, row any) bool {
	q, args := s.sqlPares(ctx, sqlStr, params)
	err := s.Db.Get(row, q, args...)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return false
	case err != nil:
		slog.ErrorContext(ctx, "mdb Query failed", "error", err.Error(), "sql", sqlStr, "data", params)
		return false
	}
	return true
}

func (s MysqlClient) SelectWithError(ctx *base.BaseContext, sqlStr string, params map[string]any, row any) bool {
	q, args := s.sqlPares(*ctx, sqlStr, params)
	err := s.Db.Get(row, q, args...)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return false
	case err != nil:
		slog.ErrorContext(*ctx, "mdb Query failed", "error", err, "sql", sqlStr, "data", params)
		(*ctx).SetError(err)
		return false
	}
	return true
}

// SelectByArgs  sqlStr="select * from t_user where userId=?" agrs: []any{"2222222"}
func (s MysqlClient) SelectByArgs(ctx base.BaseContext, sqlStr string, args []any, row any) bool {
	err := sqlx.Get(s.Db, row, sqlStr, args...)

	switch {
	case errors.Is(err, sql.ErrNoRows):
		return false
	case err != nil:
		slog.ErrorContext(ctx, "mdb Fetch StructScan failed", "error", err, "sql", sqlStr, "data", args)
		return false
	}
	return true
}

func (s MysqlClient) Fetch(ctx base.BaseContext, sqlStr string, params map[string]any, row any) bool {
	q, args := s.sqlPares(ctx, sqlStr, params)
	err := sqlx.Select(s.Db, row, q, args...)
	if err != nil {
		slog.ErrorContext(ctx, "mdb Fetch StructScan failed", "error", err, "sql", sqlStr, "data", params)
		return false
	}
	return true
}

// sqlStr="select * from t_user where userId=?" agrs: []any{"2222222"}
func (s MysqlClient) FetchByArgs(ctx base.BaseContext, sqlStr string, args []any, row any) bool {
	err := sqlx.Select(s.Db, row, sqlStr, args...)

	if err != nil {

		slog.ErrorContext(ctx, "mdb Fetch StructScan failed", "error", err, "sql", sqlStr, "data", args)
		return false
	}
	return true
}

// map[string]any{"tableName": "t_user",  "Set":map[string]any{"nick": "nihao"}, "Where":map[string]any{"userId": "1111"}}
// 下面你的跟新方法 可以按照户指定顺序更新字段,  有些时候需要指定更新顺序的 就用下买你的方法传入参数
// map[string]any{"tableName": "t_user",  "Set":[]map[string]any{{"nick": "nihao"}, {"name": []string{"if(s=0, 1, 0)"}}}, "Where":map[string]any{"userId": "1111"}}
func (s MysqlClient) Update(ctx base.BaseContext, params map[string]any, tx ...*sqlx.Tx) int64 {
	tableName := s.getTableName(ctx, params)

	var sqlStr string = "update " + tableName
	var tempParams = make(map[string]any, 0)
	setL := make([]string, 0)

	switch params["Set"].(type) {
	case map[string]any:
		setValues := params["Set"].(map[string]any)
		for k, v := range setValues {
			tpv := ""
			if vt, ok := v.([]string); ok {
				tpv = "`" + k + "`" + " = " + vt[0]
			} else {
				tpv = "`" + k + "`" + "=" + " :" + k
				tempParams[k] = v
			}
			setL = append(setL, tpv)
		}
	case []map[string]any:
		setValues := params["Set"].([]map[string]any)
		for _, sv := range setValues {
			for k, v := range sv {
				tpv := ""
				if vt, ok := v.([]string); ok {
					tpv = "`" + k + "`" + " = " + vt[0]
				} else {
					tpv = "`" + k + "`" + "=" + " :" + k
					tempParams[k] = v
				}
				setL = append(setL, tpv)
			}
		}
	}

	// insert into t_user set ss=w
	whereValues := params["Where"].(map[string]any)
	wvL := make([]string, 0)
	var tpv string
	for k, v := range whereValues {
		if reflect.TypeOf(v).Kind() == reflect.Slice {
			tpv = "`" + k + "`" + " in" + "( :" + k + " )"

		} else {
			tpv = "`" + k + "`" + "=" + " :" + k
		}
		tempParams[k] = v
		wvL = append(wvL, tpv)
	}
	sqlStr = sqlStr + " set " + strings.Join(setL, ", ") + " where " + strings.Join(wvL, " and ")
	q, args := s.sqlPares(ctx, sqlStr, tempParams)
	var rs sql.Result
	var err error
	if len(tx) > 0 && tx[0] != nil {
		rs, err = tx[0].Exec(q, args...)
	} else {
		rs, err = s.Db.Exec(q, args...)
	}

	if err != nil {
		paS, _ := json.Marshal(args)
		slog.ErrorContext(ctx, "mdb update failed", "error", err, "sql", q, "data", string(paS))
		(ctx).SetError(err)
		return -1
	}
	aft, _ := rs.RowsAffected()
	return aft
}

// map[string]any{"tableName":"t_user", "Set":map[string]any, "Update":map[string]any}
// 下面你的跟新方法 可以按照户指定顺序更新字段,  有些时候需要指定更新顺序的 就用下买你的方法传入参数
// map[string]any{"tableName":"t_user", "Set":map[string]any, "Update":[]map[string]any}
// 更新 操作没有特别处理, update 处就传入 []string
// map[string]any{"tableName":"t_user", "Set":map[string]any, "Update":[]string}
func (s MysqlClient) InsertUpdate(ctx base.BaseContext, params map[string]any, tx ...*sqlx.Tx) sql.Result {
	tableName := s.getTableName(ctx, params)
	var sqlStr string = "insert into " + tableName

	setValues := params["Set"].(map[string]any)
	var attrs = []string{}
	var attrValues = []string{}
	for k, _ := range setValues {
		attrs = append(attrs, "`"+k+"`")
		value := ":" + k
		attrValues = append(attrValues, value)
	}
	attrString := strings.Join(attrs, ", ")
	attrValuesString := strings.Join(attrValues, ", ")
	sqlStr = sqlStr + "( " + attrString + ")" + "  values( " + attrValuesString + " )"

	var UpdateL = make([]string, 0)
	if uValues, ok := params["Update"].(map[string]any); ok {
		for k, v := range uValues {
			tpv := ""
			if vt, ok := v.([]string); ok {
				tpv = "`" + k + "`" + " = " + vt[0]
			} else {
				tpv = "`" + k + "`" + "=values(`" + k + "`)"
			}
			UpdateL = append(UpdateL, tpv)
		}
	} else if uValues, ok := params["Update"].([]map[string]any); ok {
		for _, sv := range uValues {
			for k, v := range sv {
				tpv := ""
				if vt, ok := v.([]string); ok {
					tpv = "`" + k + "`" + " = " + vt[0]
				} else {
					tpv = "`" + k + "`" + "=values(`" + k + "`)"
				}
				UpdateL = append(UpdateL, tpv)
			}
		}
	} else if uValues, ok := params["Update"].([]string); ok {
		for _, name := range uValues {
			tpv := ""
			tpv = "`" + name + "`" + "=values(`" + name + "`)"
			UpdateL = append(UpdateL, tpv)
		}
	}
	sqlStr += " on duplicate key update " + strings.Join(UpdateL, ",")
	var rs sql.Result
	var err error
	if len(tx) > 0 && tx[0] != nil {
		rs, err = tx[0].NamedExec(sqlStr, setValues)
	} else {
		rs, err = s.Db.NamedExec(sqlStr, setValues)
	}
	if err != nil {
		slog.ErrorContext(ctx, "mdb insert failed", "error", err.Error(), "sql", sqlStr, "data", params)
		(ctx).SetError(err)
		return nil
	}

	return rs
}

// map[string]any{"DB": "", "tableName":"t_user", "name": "nick", "id": 1}
func (s MysqlClient) Insert(ctx base.BaseContext, params map[string]any, tx ...*sqlx.Tx) sql.Result {
	tableName := s.getTableName(ctx, params)
	var sqlStr string = "insert into " + tableName
	var attrs = []string{}
	var attrValues = []string{}
	for k, _ := range params {
		attrs = append(attrs, "`"+k+"`")
		value := ":" + k
		attrValues = append(attrValues, value)
	}
	attrString := strings.Join(attrs, ", ")
	attrValuesString := strings.Join(attrValues, ", ")
	sqlStr = sqlStr + "( " + attrString + ")" + "  values( " + attrValuesString + " )"

	var rs sql.Result
	var err error
	if len(tx) > 0 && tx[0] != nil {
		rs, err = tx[0].NamedExec(sqlStr, params)
	} else {
		rs, err = s.Db.NamedExec(sqlStr, params)
	}
	if err != nil {
		slog.ErrorContext(ctx, "mdb insert failed", "error", err.Error(), "sql", sqlStr, "data", params)
		ctx.SetError(err)
		//  ctx.Error.(*mysql.MySQLError), 可以获取对应的Number,  mysql 不同的number 有不同的定义 1062 就是唯一键约束
		return nil
	}

	return rs
}

// InsertMany map[string]any{"DB": "", "tableName":"t_user",  "Set":[{"name": "nick", "id": 1}] || {{"name": "nick", "id": 1}}}
// 这个支持插入单挑数据  或多条数据
func (s MysqlClient) InsertMany(ctx base.BaseContext, params map[string]any, tx ...*sqlx.Tx) sql.Result {
	tableName := s.getTableName(ctx, params)
	var sqlStr string = "insert into " + tableName
	var attrs = []string{}
	allValues := params["Set"] // []map[string]any
	finalVStr := ""
	if ap, ok := allValues.(map[string]any); ok {
		var attrValues = []string{}
		for k, _ := range ap {
			attrs = append(attrs, "`"+k+"`")
			value := ":" + k
			attrValues = append(attrValues, value)
		}
		finalVStr = "(" + strings.Join(attrValues, ", ") + ")"

	} else {
		if aps, ok := allValues.([]any); ok {
			for _, ap := range aps {
				var attrValues = []string{}
				insertData, ok := ap.(map[string]any)
				if !ok {
					slog.ErrorContext(ctx, "mysql Parameter error", "data", allValues, "need", "[]any")
					break
				}
				for k, _ := range insertData {
					attrs = append(attrs, "`"+k+"`")
					value := ":" + k
					attrValues = append(attrValues, value)
				}
				finalVStr = "(" + strings.Join(attrValues, ", ") + ")"
				break
			}
		} else if aps, ok := allValues.([]map[string]any); ok {
			for _, insertData := range aps {
				var attrValues = []string{}
				if !ok {
					slog.ErrorContext(ctx, "mysql Parameter error", "data", allValues, "need", "[]any")
					break
				}
				for k, _ := range insertData {
					attrs = append(attrs, "`"+k+"`")
					value := ":" + k
					attrValues = append(attrValues, value)
				}
				finalVStr = "(" + strings.Join(attrValues, ", ") + ")"
				break
			}
		}
	}

	attrString := strings.Join(attrs, ", ")
	sqlStr = sqlStr + "( " + attrString + ")" + "  values " + finalVStr

	var rs sql.Result
	var err error
	if len(tx) > 0 && tx[0] != nil {
		rs, err = tx[0].NamedExec(sqlStr, allValues)
	} else {
		rs, err = s.Db.NamedExec(sqlStr, allValues)
	}
	if err != nil {
		slog.ErrorContext(ctx, "mdb insert failed", "error", err.Error(), "sql", sqlStr, "data", allValues)
		ctx.SetError(err)
		return nil
	}

	return rs
}

func (s MysqlClient) Execute(ctx base.BaseContext, sqlStr string, params map[string]any, tx ...*sqlx.Tx) sql.Result {
	//不能做查询， 这里是没有返回结果的
	q, args := s.sqlPares(ctx, sqlStr, params)
	var rs sql.Result
	var err error
	if len(tx) > 0 && tx[0] != nil {
		rs, err = tx[0].Exec(q, args...)
	} else {
		rs, err = s.Db.Exec(q, args...)
	}
	if err != nil {
		slog.ErrorContext(ctx, "mdb Execute failed", "error", err, "sql", q, "data", params)
		(ctx).SetError(err)
		return nil
	}
	return rs
}

func (s MysqlClient) Transaction(ctx base.BaseContext, queryObj func(base.BaseContext, MysqlClient, *sqlx.Tx)) (err error) {

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
		} else if (ctx).GetError() != nil {
			err = beginx.Rollback()
			slog.ErrorContext(ctx, "事务回滚", "error", err)
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
	queryObj(ctx, s, beginx)
	return
}
