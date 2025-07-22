package mdb

// use

// 1. 调用 SetBinlogTable(),  配置表， 以及表数据怎样处理

// 2 有配置文件   配置文件的写法
//
//	"binlog": {
//		 "addr": "host:port",
//		 "password": "xxxxxx",
//		 "user": "xxx",
//		 "db": "xxx"
//	}
// 然后直接 调用 Run()

// 2 没有配置文件 就在 调用 Run(binlogConfig) 函数时 传入实例化的 BinlogConfig

import (
	"encoding/json"
	"fmt"
	"github.com/duke-git/lancet/v2/slice"
	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/panjf2000/ants/v2"
	"io"
	"log/slog"
	"os"
	"runtime/debug"
)

type BinlogConfig struct {
	Addr       string `json:"addr"` // "127.0.0.1:13306"
	Password   string `json:"password"`
	User       string `json:"user"`
	Db         string `json:"db"`
	UseHistory bool   `json:"useHistory"` // 是否继续上次结束位置开始， true的时候 下面的 Position 生效， 否侧无效
	Position   string `json:"position"`   // 同步的位数据置保存路径， 默认文件名binlog_position.txt
}

type Action string

var (
	Update Action = "update"
	Delete Action = "delete"
	Insert Action = "insert"
)

type BinLogClient struct {
	canal.DummyEventHandler
	Canal    *canal.Canal
	Config   BinlogConfig
	goPool   *ants.Pool
	isClosed bool
}

func NewBingLogClient(config BinlogConfig) *BinLogClient {
	pool, err := ants.NewPool(1000, ants.WithNonblocking(true))
	if err != nil {
		slog.Error("设置携程池失败", err.Error())
	}
	if config.Position == "" {
		config.Position = "binlog_position.txt"
	}
	return &BinLogClient{
		Config: config,
		goPool: pool,
	}
}

// ATable 外部table结构体需要 实现这个接口
// 内部参数map[string]any解析到结构体
//
//	err := utils.MapStructConvertWithTag(data, &table, "json", true)
//	if err != nil {
//		slog.Error("binlog error", "error", err.Error())
//	}
type ATable interface {
	Delete(data ...map[string]any)
	Update(data ...map[string]map[string]any)
	Insert(data ...map[string]any)
	GetTableName() string // tablename 前面要加上dbname          "dbname.tablename"
	GetListenAction() []Action
}

type TTData struct {
	TableName string
	Table     ATable
	Action    []Action
}

var table = make(map[string]TTData)

// SetTable 设置table
func SetTable(tb ATable) {
	tableName := tb.GetTableName()
	action := tb.GetListenAction()
	table[tableName] = TTData{TableName: tableName, Action: action, Table: tb}
}

func (e *BinLogClient) SetTable(tb ATable) {
	tableName := tb.GetTableName()
	action := tb.GetListenAction()
	table[tableName] = TTData{TableName: tableName, Action: action, Table: tb}
}

func (h *BinLogClient) OnRow(e *canal.RowsEvent) error {
	t, ok := table[e.Table.Schema+"."+e.Table.Name]
	if !ok {
		return nil
	}
	if !slice.Contain(t.Action, Action(e.Action)) {
		return nil
	}
	var EnumMap map[int][]string = make(map[int][]string)
	columsName := make([]string, len(e.Table.Columns))
	for i, v := range e.Table.Columns {
		columsName[i] = v.Name
		if v.Type == 3 {
			EnumMap[i] = make([]string, 0)
			EnumMap[i] = append(append(EnumMap[i], ""), v.EnumValues...)
		}
	}
	targetData := make([]map[string]any, len(e.Rows))
	for i, edata := range e.Rows {
		for k, v := range EnumMap {
			if ev := edata[k]; ev != nil {
				edata[k] = v[ev.(int64)]
			}
		}
		//dd, _ := json.Marshal(data)   // 使用json 处理 []uint8 的数据会有问题
		var data = map[string]any{}
		for i, k := range columsName {
			data[k] = edata[i]
		}
		targetData[i] = data
	}

	// 保存数据 操作 由用户自己决定
	switch e.Action {
	case "insert":
		h.goPool.Submit(func() {
			defer func() {
				if err := recover(); err != nil {
					slog.Error("insert error", "data", targetData, "error", debug.PrintStack)
				}
			}()
			t.Table.Insert(targetData...)
		})
	case "delete":
		h.goPool.Submit(func() {
			defer func() {
				if err := recover(); err != nil {
					slog.Error("delete error", "data", targetData, "error", debug.PrintStack)
				}
			}()
			t.Table.Delete(targetData...)
		})
	case "update":

		var updateInfo []map[string]map[string]any = make([]map[string]map[string]any, len(targetData)/2)
		for i := 0; i < len(targetData)/2; i++ {
			updateInfo[i] = map[string]map[string]any{
				"old": targetData[i*2],
				"new": targetData[i*2+1],
			}
		}
		go h.goPool.Submit(func() {
			defer func() {
				if err := recover(); err != nil {
					slog.Error("update error", "data", targetData, "error", debug.PrintStack)
				}
			}()
			t.Table.Update(updateInfo...)
		})
	}

	return nil
}

func (h *BinLogClient) String() string {
	return "MyEventHandler"
}

// Run 在调用这个函数之前 必须先调用  SetTable  设置需要监听的 表， 一旦启动就没法在设置了
func (h *BinLogClient) Run() {
	cfg := canal.NewDefaultConfig()
	cfg.Addr = h.Config.Addr
	cfg.Password = h.Config.Password
	cfg.User = h.Config.User
	cfg.Charset = "utf8mb4"
	cfg.Dump.ExecutionPath = ""
	cfg.Logger = slog.Default()
	for _, v := range table {
		cfg.IncludeTableRegex = append(cfg.IncludeTableRegex, fmt.Sprintf("^%s$", v.TableName))
	}
	slog.Info("binglog config", "config", h.Config, "tables", cfg.IncludeTableRegex)
	c, err := canal.NewCanal(cfg)
	if err != nil {
		slog.Error(err.Error())
	}
	h.Canal = c

	c.SetEventHandler(h)

	slog.Info("开启 binglo 监听", "config", h.Config)
	// Start canal
	go h.pos()
}

func (h *BinLogClient) pos() {
	var pos mysql.Position
	if h.Config.UseHistory {
		if posStr, err := ReadFile(h.Config.Position); err != nil {
			slog.Error("读取历史位置错误", err.Error())
		} else {
			if posStr != nil {
				err := json.Unmarshal(posStr, &pos)
				if err != nil {
					slog.Error("解析binlog历史位置错误", "error", err.Error())
				}
			}
		}

		if pos.Pos > 0 && pos.Name != "" {
			// 直接用这个数据开始
			err := h.Canal.RunFrom(pos)
			if err != nil {
				slog.Error("run binlog error 001", "error", err.Error())
			}
		}
		if h.isClosed {
			return
		}
	}

	// 没有历史数据的， 就使用最新的数据
	pos, err := h.Canal.GetMasterPos()
	if err != nil {
		slog.Error("获取binlog 最新日志位置失败")
	}
	err = h.Canal.RunFrom(pos)
	if err != nil {
		slog.Error("run binlog error 002", "error", err.Error())
	}
}

func (h *BinLogClient) Close() {
	h.isClosed = true
	h.Canal.Close()
	pos := h.Canal.SyncedPosition()
	marshal, _ := json.Marshal(pos)
	if len(marshal) == 0 {
		return
	}
	file, err := os.OpenFile(h.Config.Position, os.O_CREATE|os.O_RDWR, os.FileMode(0644))
	if err != nil {
		slog.Error("打开文件失败", "error", err.Error())
	}
	defer file.Close()
	_ = file.Truncate(0)
	file.Write(marshal)
	file.Sync()
}

func ReadFile(filePath string) ([]byte, error) {
	file, err := os.Open(filePath)
	if err != nil {
		slog.Error("file open fail", "file_name", filePath, "error", err.Error())
		return nil, err
	}
	defer file.Close()
	fd, err := io.ReadAll(file)
	if err != nil {
		slog.Error("read to fd fail", "error", err.Error())
		return nil, err
	}
	return fd, nil
}
