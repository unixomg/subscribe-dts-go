package schedular

import (
	"fmt"
	"goimpl/common"
	"goimpl/config"
	"goimpl/db"
	"goimpl/dumper"
	"goimpl/kafkaconsumer"
	"goimpl/logger"
	"os"
	"os/signal"
	"time"

	"go.uber.org/zap"
)

type Executor struct {
}

var (
	G_Executor *Executor
)

func newExecutingZapLog(env, MonitorTableItem string) *zap.Logger {
	var (
		e_zaplogger *zap.Logger
		err         error
	)
	logType := fmt.Sprintf("GO-%s", MonitorTableItem)
	e_zaplogger, err = logger.InitGoroutingLogger(config.Conf.LogConfig, logType, env)
	if err != nil {
		logger.Error("Init Gorouting ZapLogger error:", zap.Any("newExecutingZapLog", err))
	}
	return e_zaplogger

}

func (e *Executor) Execute(consumerItem *kafkaconsumer.KafkaConsumerItem) {
	signal.Notify(consumerItem.ExitExecutorSignal, os.Kill)

	//0、init Goroutine logger
	e_zaplogger := newExecutingZapLog(consumerItem.Env, consumerItem.MonitorTableItem)

	//1、exec table dump
	if err := dumper.Tablefulldump(consumerItem.Env, consumerItem.MonitorTableItem, e_zaplogger); err != nil {
		e_zaplogger.Error("Tablefulldump Error:", zap.Any("error", err))

	}
	//2、exec dest table import
	if err := dumper.Tablefullimport(consumerItem.Env, consumerItem.MonitorTableItem, e_zaplogger); err != nil {
		e_zaplogger.Error("Tablefullimport Error:", zap.Any("error", err))
	}

	//3、start kafka consumer goroutine
	kafkaTopic := make([]string, 0)
	kafkaTopic = append(kafkaTopic, common.KafkaTopic)
	kafkaBrokerList := make([]string, 0)
	kafkaBrokerList = append(kafkaBrokerList, common.KafkaBroker)
	dbObj := db.GetDbObj()
	key := fmt.Sprintf("consumergroupinfo-%s", db.Olap)
	kafkaGroupId, _ := redisClient.HGet(key, consumerItem.MonitorTableItem).Result()

	e_zaplogger.Info("...Eexecute Start ConsumerGroupId is:", zap.Any("kafkaGroupId", kafkaGroupId))
	time.Sleep(5 * time.Second)
	go consumerItem.KafkaConsumerProcess(common.KafkaUser, common.KafkaPassword, kafkaTopic, kafkaGroupId, kafkaBrokerList, dbObj, e_zaplogger)
	select {
	case <-consumerItem.ExitExecutorSignal:
		e_zaplogger.Sync()
		e_zaplogger.Info("Executor Gorouting Quit...", zap.Any("tableName", consumerItem.MonitorTableItem))
		return
	}
}
func InitExecutor() {
	G_Executor = &Executor{}
	return
}
