package dumper

import (
	"errors"
	"fmt"
	"goimpl/alisdk"
	"goimpl/config"
	"goimpl/db"
	"goimpl/utils"
	"strconv"
	"time"

	"go.uber.org/zap"
	"xorm.io/xorm/schemas"
)

func dumptable(tablename, filePath string, xormClient *db.Mysqlins) error {
	metaTableList, err := xormClient.DBMetas()
	if err != nil {
		return err
	}
	dumpTableObjList := make([]*schemas.Table, 0)
	for _, tableObj := range metaTableList {
		if tableObj.Name == tablename {
			dumpTableObjList = append(dumpTableObjList, tableObj)
		}
	}
	err = xormClient.DumpTablesToFile(dumpTableObjList, filePath, schemas.MYSQL)
	if err != nil {
		return err
	}
	return nil
}

func checkDtsSyncConfigStatus(cfg map[string]interface{}, dbName, tablename string) (opstatus bool, err error) {
	err, dtsDescInsStatus := alisdk.DescribeSubscriptionInstanceStatus(cfg["subscriptioninstanceid"].(string))
	if err != nil {
		return false, err
	}
	alreadyTableConfigList := make([]string, 0)
	updateTableConfigList := make([]string, 0)
	if dtsDescInsStatus.Status == "Normal" {
		for _, Obj := range dtsDescInsStatus.SubscriptionObject.SynchronousObject {
			alreadyTableConfigList = Obj.TableList.Table
			isExist := utils.IsExistInStrArray(tablename, alreadyTableConfigList)
			if isExist == false {
				updateTableConfigList = append(updateTableConfigList, tablename)
			}
		}
	}
	if len(updateTableConfigList) > 0 {
		allConfigsList := utils.Union(alreadyTableConfigList, updateTableConfigList)
		//update dts subinstance config
		err, modifyResp := alisdk.ModifySubscriptionObject(cfg["subscriptioninstanceid"].(string), dbName, allConfigsList)
		if err != nil {
			return false, err
		}
		if modifyResp.Success == true {
			//fmt.Println("modify Config Success")
			return true, nil
		}
	}
	return false, err
}

func Tablefulldump(env, tablename string, e_zaplogger *zap.Logger) (err error) {
	var (
		redisClient *db.RedisIns
		xormClient  *db.Mysqlins
	)
	switch env {
	case "dev":
		redisClient = db.DevRedis()
		xormClient = db.GetterOlapMysql()
	}
	dumpKey := fmt.Sprintf("%s-%s", env, tablename)
	res := redisClient.Exists(dumpKey)
	val, err := res.Result()
	if err != nil {
		return err
	}
	//fmt.Println("Tablefulldump....",val,err)
	cfg := config.GetAcmConfig().GetStringMap("dts.subscription." + env)
	consumergroupname := fmt.Sprintf("%s", tablename)
	consumerGroupInfo := fmt.Sprintf("consumergroupinfo-%s", db.Olap)
	dbName := fmt.Sprintf("%s", db.Olap)

	if val == 0 {

		//1、Execute...
		e_zaplogger.Info("first fulldump")
		timestampStr := strconv.Itoa(int(time.Now().Unix()))
		fullDumpSqlFileName := fmt.Sprintf("%s-%s-%s", env, tablename, timestampStr)
		filePath := fmt.Sprintf("dumps/%s", fullDumpSqlFileName)
		//Call CreateConsumerGroup func
		err, dtsres := alisdk.CreateConsumerGroup(cfg["subscriptioninstanceid"].(string), consumergroupname, cfg["consumergroupusername"].(string), cfg["consumergrouppassword"].(string))
		if err != nil {
			return err
		}
		//Save CreateConsumerGroup Info
		_, err = redisClient.HSet(consumerGroupInfo, consumergroupname, dtsres.ConsumerGroupID).Result()
		if err != nil {
			e_zaplogger.Error("Redis HSet consumerGroupInfo error:", zap.Any("error", err))
		}
		if dtsres.Success == true {
			err = dumptable(tablename, filePath, xormClient)
			if err != nil {
				e_zaplogger.Error("Dumptable error", zap.Any("error", err))
				return err
			} else {
				res := redisClient.HSet(dumpKey, "dumptime", timestampStr, "filepath", filePath)
				if res.Err() != nil {
					e_zaplogger.Error("Redis HSet dumpKey error:", zap.Any("error", err))
					return err
				}
			}
			time.Sleep(5 * time.Second)
		}
		//2、check dts sync table object config
		status, err := checkDtsSyncConfigStatus(cfg, dbName, tablename)
		if err != nil {
			e_zaplogger.Error("CheckDtsSyncConfigStatus error:", zap.Any("error", err))

		}
		if status == true {
			e_zaplogger.Info("Update DTS Status OK")
		} else {
			e_zaplogger.Info("Nothing Need to Update")
		}

	} else {
		e_zaplogger.Info("...Second Dump .......")
		err, dtsdescresp := alisdk.DescribeConsumerGroup(cfg["subscriptioninstanceid"].(string))
		if err != nil {
			return err
		}
		flag := false
		for _, item := range dtsdescresp.ConsumerChannels.DescribeConsumerChannel {
			//fmt.Println("DescribeConsumerChannel",item.ConsumerGroupName)
			if item.ConsumerGroupName != consumergroupname {
				flag = false
			} else {
				//TODO Reset CheckPoint
				flag = true
				break

			}
		}
		if flag == false {
			//create CreateConsumerGroup
			err, dtsres := alisdk.CreateConsumerGroup(cfg["subscriptioninstanceid"].(string), consumergroupname, cfg["consumergroupusername"].(string), cfg["consumergrouppassword"].(string))
			if err != nil {
				return err
			}
			//Save CreateConsumerGroup Info
			_, err = redisClient.HSet(consumerGroupInfo, consumergroupname, dtsres.ConsumerGroupID).Result()
			if err != nil {
				e_zaplogger.Error("Redis HSet consumerGroupInfo error:", zap.Any("error", err))
			}
			if dtsres.Success == true {
				time.Sleep(5 * time.Second)
			}
		}

		//check dts subinstance config is ok
		status, err := checkDtsSyncConfigStatus(cfg, dbName, tablename)
		if err != nil {
			e_zaplogger.Error("CheckDtsSyncConfigStatus error:", zap.Any("error", err))
		}
		if status == true {
			e_zaplogger.Info("Update DTS Status OK")
		} else {
			e_zaplogger.Info("Nothing Need to Update")
		}

		//any flag be execute dumptable
		timestampStr := strconv.Itoa(int(time.Now().Unix()))
		fullDumpSqlFileName := fmt.Sprintf("%s-%s-%s", env, tablename, timestampStr)
		filePath := fmt.Sprintf("dumps/%s", fullDumpSqlFileName)
		err = dumptable(tablename, filePath, xormClient)
		if err != nil {
			e_zaplogger.Error("Dumptable error", zap.Any("error", err))
			return err
		} else {
			res := redisClient.HSet(dumpKey, "dumptime", timestampStr, "filepath", filePath)
			if res.Err() != nil {
				e_zaplogger.Error("Redis HSet dumpKey error:", zap.Any("error", err))
				return err
			}
		}

	}
	return nil
}

func Tablefullimport(env, tablename string, e_zaplogger *zap.Logger) (err error) {
	var (
		redisClient    *db.RedisIns
		xormClient     *db.Mysqlins
		importfilepath string
	)
	switch env {
	case "dev":
		redisClient = db.DevRedis()
		xormClient = db.GetterLocalMysql()
	}
	dumpKey := fmt.Sprintf("%s-%s", env, tablename)
	res := redisClient.Exists(dumpKey)
	val, err := res.Result()
	if err != nil {
		return err
	}
	if val == 0 {
		return errors.New("No can import table dumps")
	} else {
		res := redisClient.HGet(dumpKey, "filepath")
		if res.Err() != nil {
			return res.Err()
		} else {
			importfilepath = res.Val()
		}
		v, err := utils.PathExists(importfilepath)
		if v == false || err != nil {
			return err
		} else if v == true && err == nil {
			resultlist, err := xormClient.ImportFile(importfilepath)
			if err != nil {
				e_zaplogger.Error("ImportFile error", zap.Any("error", err))
				return err
			}
			lastSqlResult := resultlist[len(resultlist)-1]
			lastInsertId, _ := lastSqlResult.LastInsertId()
			lastRowAffect, _ := lastSqlResult.RowsAffected()
			//fmt.Println("LastInsertId:",lastInsertId,"RowsAffected:",lastRowAffect)
			e_zaplogger.Info("Import Result Info:", zap.Any("LastInsertId", lastInsertId), zap.Any("RowsAffected", lastRowAffect))
			importKey := fmt.Sprintf("%s-%s-importstat", env, tablename)
			res := redisClient.HSet(importKey, "LastInsertId", lastInsertId, "RowsAffected", lastRowAffect, "Status", "OK")
			if res.Err() != nil {
				e_zaplogger.Error("Redis Hset importKey error", zap.Any("error", err))
				return res.Err()
			}
		}
	}
	return nil
}
