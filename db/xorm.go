package db

import (
	"fmt"
	"goimpl/config"

	"xorm.io/xorm"
)

const (
	Localdb = "localdb"
	Olap    = "olap"
)

type xormItem struct {
	Local      *xorm.Engine
	BoxianOlap *xorm.Engine
}

var xi xormItem

type Mysqlins struct {
	*xorm.Engine
}

func GetterLocalMysql() *Mysqlins {
	return &Mysqlins{
		xi.Local,
	}
}

func GetterOlapMysql() *Mysqlins {
	return &Mysqlins{
		xi.BoxianOlap,
	}
}

func InitXormIns(names ...string) (err error) {
	for _, name := range names {
		xormins, err := initXorm(name)
		if err != nil {
			return err
		}
		switch name {
		case Localdb:
			xi.Local = xormins

		case Olap:
			xi.BoxianOlap = xormins

		}
	}
	return nil
}

func CloseXorm(names ...string) {
	for _, name := range names {
		switch name {
		case Localdb:
			xi.Local.Close()
		case Olap:
			xi.BoxianOlap.Close()
		}

	}
}

func initXorm(name string) (*xorm.Engine, error) {
	cfg := config.GetAcmConfig().GetStringMap("database.mysql." + name)
	fmt.Printf("%+v\n", cfg)
	//dbConfigStr := fmt.Sprintf("%s:%s@tcp(%s:3306)/%s?loc=Local&timeout=5s&charset=utf8mb4&collation=utf8mb4_unicode_ci&interpolateParams=true&parseTime=true",
	//	cfg["username"],
	//	cfg["password"],
	//	cfg["host"],
	//	cfg["database"],
	//)
	dbConfigStr := fmt.Sprintf("%s:%s@tcp(%s:3306)/%s?timeout=5s&charset=utf8mb4&collation=utf8mb4_unicode_ci&interpolateParams=true&parseTime=true",
		cfg["username"],
		cfg["password"],
		cfg["host"],
		cfg["database"],
	)

	driver := fmt.Sprintf("%s", cfg["driver"])
	engine, err := xorm.NewEngine(driver, dbConfigStr)
	if err != nil {
		return nil, err
	}
	err = engine.Ping()
	if err != nil {
		return nil, err
	}
	return engine, nil
}
