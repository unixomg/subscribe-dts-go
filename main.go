package main

import (
	"fmt"
	"goimpl/config"
	"goimpl/db"
	"goimpl/logger"
	"goimpl/schedular"
	"goimpl/worker"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"go.uber.org/zap"
)

func main() {

	env := "dev"
	logType := "main"
	var (
		m   *sync.Mutex
		err error
	)
	m = new(sync.Mutex)

	signals_main := make(chan os.Signal, 1)
	signal.Notify(signals_main, syscall.SIGINT, syscall.SIGTERM, os.Interrupt)

	// init Conf
	if err = config.InitFromIni("conf/conf.ini"); err != nil {
		fmt.Printf("init conf failed, err:%v\n", err)
		panic(err)
	}

	//init logger
	if err = logger.InitLogger(config.Conf.LogConfig, logType, env); err != nil {
		fmt.Printf("init logger failed, err:%v\n", err)
		panic(err)

	}
	defer logger.Sync()

	//init dest op DbConnection

	if err = db.InitDb(config.Conf.Username, config.Conf.Password, config.Conf.Host, config.Conf.Port, config.Conf.DB); err != nil {
		logger.Error("init db error:", zap.Any("error", err))
		panic(err)
	}
	defer db.DbObj.Closer()

	//init ACM
	if err = config.InitAcmItem(env, config.AcmItem{
		Id:    "database",
		Group: "DEFAULT_GROUP",
	}, config.AcmItem{
		Id:    "dts",
		Group: "dtsservice",
	}); err != nil {
		logger.Error("init acm error:", zap.Any("error", err))
		panic(err)
	}
	time.Sleep(10 * time.Second)

	//init xorm
	if err = db.InitXormIns(db.Olap, db.Localdb); err != nil {
		logger.Error("init xorm error:", zap.Any("error", err))
		panic(err)
	}
	defer func() {
		db.CloseXorm(db.Olap, db.Localdb)
	}()

	//init Redis
	if err = db.InitRedisIns(db.RedisDev); err != nil {
		logger.Error("init redis error:", zap.Any("error", err))
		panic(err)
	}
	defer func() {
		db.CloseRedis(db.RedisDev)
	}()

	//init Executor
	schedular.InitExecutor()

	//init Schedular
	if err = schedular.InitSchedular(env); err != nil {
		logger.Error("schedular init failed:", zap.Any("error", err))
		panic(err)
	}

	lisObj := worker.InitListenObj("synctables", "monitor")

	if err := lisObj.ListenConfig(m, env); err != nil {
		fmt.Println(err)
		logger.Error("ListenConfig occur error:", zap.Any("error", err))
	}

	//go func() {
	//	select {
	//	case <-signals_main:
	//		logger.Info("Main Process Quit....")
	//		fmt.Println("Main quit")
	//		os.Exit(0)
	//	}
	//}()
	//select {}
	<-signals_main

}
