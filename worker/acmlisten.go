package worker

import (
	"fmt"
	"goimpl/common"
	"goimpl/config"
	"goimpl/db"
	"goimpl/logger"
	"goimpl/schedular"
	"goimpl/utils"
	"strings"
	"sync"

	"github.com/nacos-group/nacos-sdk-go/clients/config_client"
	"github.com/nacos-group/nacos-sdk-go/vo"
	"github.com/spf13/viper"
	"go.uber.org/zap"
)

type listenObj struct {
	acmItem      config.AcmItem
	v            *viper.Viper
	configClient config_client.IConfigClient
	exitchan     chan bool
}

func InitListenObj(dataId, group string) *listenObj {
	return &listenObj{
		acmItem: config.AcmItem{
			dataId,
			group,
		},
		v:            config.GetAcmConfig(),
		configClient: config.GetConfigClient(),
		exitchan:     make(chan bool),
	}
}

func (l *listenObj) ListenConfig(m *sync.Mutex, env string) (err error) {
	var (
		redisClient *db.RedisIns
	)
	switch env {
	case "dev":
		redisClient = db.DevRedis()
	}
	destDb := config.Conf.DB
	err = l.configClient.ListenConfig(vo.ConfigParam{
		DataId: l.acmItem.Id,
		Group:  l.acmItem.Group,
		OnChange: func(namespace, group, dataId, data string) {
			m.Lock()
			defer m.Unlock()
			logger.Debug("ListenConfig:", zap.Any("group", group), zap.Any("dataId:", dataId), zap.Any("data", data))
			if err := l.v.MergeConfig(strings.NewReader(data)); err != nil {
				logger.Error("MergeConfig error:", zap.Any("error", err))
			}
			switch dataId {
			case "synctables":
				getKey := fmt.Sprintf("listentables.%s", destDb)
				res := redisClient.Exists(common.MonitorTablesKey)
				val, err := res.Result()
				if err != nil {
					logger.Error("Redis Get MonitorTablesKey error:", zap.Any("error", err))
				}
				if val == 0 {
					for _, v := range l.v.Get(getKey).([]interface{}) {
						//First determine whether the element is in the Set collection
						isMember, err := redisClient.SIsMember(common.MonitorTablesKey, v).Result()
						if err != nil {
							logger.Error("Redis SIsMember error:", zap.Any("error", err))
						}
						if isMember == false {
							_, err := redisClient.SAdd(common.MonitorTablesKey, v).Result()
							if err != nil {
								logger.Error("Redis SAdd error:", zap.Any("error", err))
							}
							//TODO notify Schedular
							jobObj := common.BuildJobObj(v.(string), env)
							jobEvent := common.BuildJobEvent(common.JOB_EVENT_CONSUMER_START, jobObj)
							schedular.G_Schedular.PushJobEvent(jobEvent)
						} else {
							continue
						}
					}
				} else {
					//redis set members list
					membersList, err := redisClient.SMembers(common.MonitorTablesKey).Result() //[1,2,3]
					//fmt.Println("MonitormembersList",membersList)
					if err != nil {
						logger.Error("Redis get SMember error:", zap.Any("error", err))
					}
					//acm listentable list
					configList := l.v.Get(getKey).([]interface{})
					if configList != nil {
						configStringList, err := utils.ConvertoStringSlice(configList)
						if err != nil {
							logger.Error("ConvertoStringSlice error:", zap.Any("error", err), zap.Any("configList", configList))
						}
						//get diff
						if len(membersList) > len(configStringList) {
							diff := utils.Difference(membersList, configStringList)
							for _, diffval := range diff {
								//TODO notify Scheduler Delete and redis set remove
								jobObj := common.BuildJobObj(diffval, env)
								jobEvent := common.BuildJobEvent(common.JOB_EVENT_CONSUMER_STOP, jobObj)
								schedular.G_Schedular.PushJobEvent(jobEvent)
								redisClient.SRem(common.MonitorTablesKey, diffval)
							}

						}

						if len(configStringList) > len(membersList) {
							diff := utils.Difference(configStringList, membersList)
							for _, diffval := range diff {
								//TODO notify Scheduler Add and redis set add
								jobObj := common.BuildJobObj(diffval, env)
								jobEvent := common.BuildJobEvent(common.JOB_EVENT_CONSUMER_START, jobObj)
								schedular.G_Schedular.PushJobEvent(jobEvent)
								_, err := redisClient.SAdd(common.MonitorTablesKey, diffval).Result()
								if err != nil {
									logger.Error("Redis SAdd error:", zap.Any("error", err))
								}

							}
						}

						if len(configStringList) == len(membersList) && len(configStringList) > 0 && len(membersList) > 0 {
							//TODO determine the consumer goroutine whether to start
							for _, val := range configStringList {
								jobObj := common.BuildJobObj(val, env)
								jobEvent := common.BuildJobEvent(common.JOB_EVENT_CONSUMER_CHECK, jobObj)
								schedular.G_Schedular.PushJobEvent(jobEvent)

							}
						}

					}

				}

			case "dts":
				//fmt.Println(l.v.GetStringMap("dts.subscription."+env))
				logger.Info("dts.subscription change:", zap.Any("change", l.v.GetStringMap("dts.subscription."+env)))

			case "database":
				fmt.Println(l.v.GetStringMap("database.mysql.localdb"))
				fmt.Println(l.v.GetStringMap("database.mysql.olap"))
			}
		},
	})
	if err != nil {
		l.exitchan <- true
	}
	select {
	case res := <-l.exitchan:
		if res == true {
			goto END
		}
	}
END:
	fmt.Println("ListenConfig FUNC is exit")
	return err
}
