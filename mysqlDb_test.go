package mdb

import (
	"github.com/preceeder/go/base"
	"testing"
)

func TestMysqlClient_Update(t *testing.T) {
	//s := NewMysqlClient(
	//	MysqlConfig{
	//		//Host:        "127.0.0.1",
	//		//Port:        "13306",
	//		//User:        "iuyd",
	//		//Password:    "nuyyd",
	//		//Db:          "test001",
	//		//MaxIdleCons: 2,
	//		//MaxOpenCons: 5,
	//	})
	s := MysqlClient{}
	ctx := base.Context{}
	s.Update(&ctx, map[string]any{
		"tableName": "t_user",
		"Set": []map[string]any{
			{"nick": "sss"},
			{"avatar": "mis.png"},
			{"dist": 23},
		},
		//"Set": map[string]any{
		//	"nick":   "sss",
		//	"avatar": "mis.png",
		//	"dist":   23,
		//},
		"Where": map[string]any{
			"id": 23,
		},
	})

}

func TestMysqlClient_InsertUpdate(t *testing.T) {
	s := MysqlClient{}
	ctx := base.Context{}
	s.InsertUpdate(&ctx, map[string]any{
		"tableName": "t_user",
		"Set": map[string]any{
			"nick":   "sss",
			"avatar": "mis.png",
			"dist":   23,
		},
		"Update": []map[string]any{
			{"nick": "sss"},
			{"avatar": "mis.png"},
			{"dist": 23},
		},
	})
}

func TestMysqlClient_Select(t *testing.T) {
	s := NewMysqlClient(
		MysqlConfig{
			Host:        "127.0.0.1",
			Port:        "13306",
			User:        "matchus",
			Password:    "9fdIWuOJODW55sUG",
			Db:          "match_dev01",
			MaxIdleCons: 2,
			MaxOpenCons: 5,
		})
	defer s.MysqlPoolClose()
	ctx := base.Context{}
	var iu = struct {
		Id int64 `db:"id"`
	}{}
	s.Select(ctx, "select * from t_user limit 1", nil, iu)
}
