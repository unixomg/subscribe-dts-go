package schedular

import (
	"fmt"
	"goimpl/alisdk"
	"goimpl/common"
	"goimpl/config"
	"goimpl/db"
	"goimpl/kafkaconsumer"
	"goimpl/logger"
	"goimpl/utils"
	"os"
	"time"

	"github.com/go-redis/redis/v7"
	"go.uber.org/zap"
)

type Schedular struct {
	ConsumerMapTable map[string]*kafkaconsumer.KafkaConsumerItem
	JobEventChan     chan *common.JobEvent
	ExecutingStatus  map[*kafkaconsumer.KafkaConsumerItem]bool
}

var (
	G_Schedular *Schedular
	redisClient *db.RedisIns
)

func (s *Schedular) handleJobEvent(jobEvent *common.JobEvent) {
	var (
		kafkaConsumerItem   *kafkaconsumer.KafkaConsumerItem
		kafkaConsumerExists bool
		dbName              string
	)
	switch jobEvent.EventType {
	case common.JOB_EVENT_CONSUMER_START:
		kafkaConsumerItem := kafkaconsumer.BuildKafkaConsumerItem(jobEvent.Job.TableName, jobEvent.Job.Env)
		s.ConsumerMapTable[jobEvent.Job.TableName] = kafkaConsumerItem
		s.ExecutingStatus[kafkaConsumerItem] = true

	case common.JOB_EVENT_CONSUMER_STOP:
		if kafkaConsumerItem, kafkaConsumerExists = s.ConsumerMapTable[jobEvent.Job.TableName]; kafkaConsumerExists {
			delete(s.ConsumerMapTable, jobEvent.Job.TableName)
		}
		tableName := kafkaConsumerItem.MonitorTableItem
		env := kafkaConsumerItem.Env
		exitConsumerSignalChan := kafkaConsumerItem.ExitConsumerSignal
		switch env {
		case "dev":
			dbName = db.Olap

		}
		//send signal
		exitConsumerSignalChan <- os.Kill
		s.ExecutingStatus[kafkaConsumerItem] = false
		status, err := s.CleanQuitResource(env, tableName, dbName)
		if err != nil {
			logger.Error("Schedular CleanQuitResource error:", zap.Any("error", err))
		}
		//del redis key
		if status == true {
			delKey := fmt.Sprintf("GOP-%s", kafkaConsumerItem.MonitorTableItem)
			redisClient.Del(delKey)
		}
		exitExecutorSignalChan := kafkaConsumerItem.ExitExecutorSignal
		//send Executor quit signal
		exitExecutorSignalChan <- os.Kill
		logger.Info("Schedular CleanQuitResource Successful")

	case common.JOB_EVENT_CONSUMER_CHECK:
		_, kafkaConsumerExists = s.ConsumerMapTable[jobEvent.Job.TableName]
		if !kafkaConsumerExists {
			kafkaConsumerItem := kafkaconsumer.BuildKafkaConsumerItem(jobEvent.Job.TableName, jobEvent.Job.Env)
			s.ConsumerMapTable[jobEvent.Job.TableName] = kafkaConsumerItem
			s.ExecutingStatus[kafkaConsumerItem] = true
		} else {
			logger.Info("Schedular JOB_CONSUMER_CHECK Status:", zap.Any("status", "consumer is already start"))
		}
	}

}

func (s *Schedular) CleanQuitResource(env, tablename, dbName string) (cleanStatus bool, err error) {
	cfg := config.GetAcmConfig().GetStringMap("dts.subscription." + env)
	err, dtsDescInsStatus := alisdk.DescribeSubscriptionInstanceStatus(cfg["subscriptioninstanceid"].(string))
	if err != nil {
		logger.Error("AliSDK DescribeSubscriptionInstanceStatus Error:", zap.Any("error", err))
		return false, err
	}
	alreadyTableConfigList := make([]string, 0)
	needCleanTableConfigList := make([]string, 0)
	if dtsDescInsStatus.Status == "Normal" {
		for _, Obj := range dtsDescInsStatus.SubscriptionObject.SynchronousObject {
			alreadyTableConfigList = Obj.TableList.Table
			isExist := utils.IsExistInStrArray(tablename, alreadyTableConfigList)
			if isExist == true {
				needCleanTableConfigList = append(needCleanTableConfigList, tablename)
			}
		}
	}

	updateConfigList := utils.Difference(alreadyTableConfigList, needCleanTableConfigList)
	if len(updateConfigList) > 0 {
		err, modifyResp := alisdk.ModifySubscriptionObject(cfg["subscriptioninstanceid"].(string), dbName, updateConfigList)
		if err != nil {
			logger.Error("AliSDK ModifySubscriptionObject Error:", zap.Any("error", err))
			return false, err
		}
		if modifyResp.Success == true {
			logger.Info("modify Config Success")
			return true, nil
		}
	}
	return false, nil
}

func (s *Schedular) TrySchedular() {

	if len(s.ConsumerMapTable) == 0 {
		time.Sleep(1 * time.Second)
		return
	}
	for _, consumerItem := range s.ConsumerMapTable {
		logger.Info("^^^^^^^^^^^^^^^TrySchedular^^^^^^^^^^^^^^")
		if s.ExecutingStatus[consumerItem] == true {
			runningKey := fmt.Sprintf("GOP-%s", consumerItem.MonitorTableItem)
			logger.Info("Ready to Start:", zap.Any("runningKey", runningKey))
			_, err := redisClient.Get(runningKey).Result()
			if err != nil {
				if err == redis.Nil {
					if _, err := redisClient.Set(runningKey, "syncing", 0).Result(); err != nil {
						logger.Error("Redis Set runningKey error:", zap.Any("error", err))
					}
					logger.Info("++++++start execute.....", zap.Any("MonitorTableItem", consumerItem.MonitorTableItem))
					go G_Executor.Execute(consumerItem)
				} else {
					logger.Error("Redis Get runningKey error:", zap.Any("error", err))
				}
			} else {
				logger.Info("consumer is already runing syncing...")
				continue
			}

		}
	}

}

func (s *Schedular) schedularLoop() {
	s.TrySchedular()
	for {
		select {
		case jobEvent := <-s.JobEventChan:
			s.handleJobEvent(jobEvent)
		}
		logger.Info("--------------schedularLoop------")
		//logger.Info("Schedular memory ConsumerMapTable:",zap.Any("ConsumerMapTable",s.ConsumerMapTable))

		s.TrySchedular()
		logger.Info("--------------schedularLoop------")
	}
}

func InitSchedular(env string) (err error) {
	switch env {
	case "dev":
		redisClient = db.DevRedis()
	}
	G_Schedular = &Schedular{
		ConsumerMapTable: make(map[string]*kafkaconsumer.KafkaConsumerItem),
		JobEventChan:     make(chan *common.JobEvent),
		ExecutingStatus:  make(map[*kafkaconsumer.KafkaConsumerItem]bool),
	}
	go G_Schedular.schedularLoop()
	return nil
}

func (s *Schedular) PushJobEvent(jobEvent *common.JobEvent) {
	s.JobEventChan <- jobEvent
}
