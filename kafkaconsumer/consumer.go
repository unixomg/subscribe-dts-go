package kafkaconsumer

import (
	"bytes"
	"github.com/Shopify/sarama"
	"github.com/actgardner/gogen-avro/v7/compiler"
	"github.com/actgardner/gogen-avro/v7/vm"
	cluster "github.com/bsm/sarama-cluster"
	"go.uber.org/zap"
	"goimpl/avro"
	"goimpl/common"
	"goimpl/db"
	"goimpl/logger"
	"goimpl/utils"
	"io"
	"os"
	"os/signal"
	"regexp"
	"strings"
	"time"
)


type KafkaConsumerItem struct {
	Env string
	MonitorTableItem string
	ExitConsumerSignal chan os.Signal
	ExitExecutorSignal chan os.Signal

}

func BuildKafkaConsumerItem(tablename string,env string) *KafkaConsumerItem {
	return &KafkaConsumerItem{
		Env: env,
		MonitorTableItem: tablename,
		ExitConsumerSignal: make(chan os.Signal,1),
		ExitExecutorSignal: make(chan os.Signal,1),
	}
}



func GetColumnDetail(t *avro.Record,e_zaplogger *zap.Logger) (columnMapList []map[string]interface{},columnValueList []interface{} ) {

	for _, j := range t.Fields.ArrayField {
		colmap := map[string]interface{}{
			"colName": j.Name,
			"colType": common.GetdataTypeName(j.DataTypeNumber),
		}
		columnMapList = append(columnMapList,colmap)
	}
	//fmt.Println(columnMapList)
	//fmt.Printf("BeforeImage: %+v \n AfterImage: %+v\n",t.BeforeImages,t.AfterImages)

	var rangeObj *avro.UnionNullStringArrayUnionNullIntegerCharacterDecimalFloatTimestampDateTimeTimestampWithTimeZoneBinaryGeometryTextGeometryBinaryObjectTextObjectEmptyObject
	if t.Operation == avro.OperationINSERT {
		rangeObj = t.AfterImages
	}else if t.Operation == avro.OperationDELETE {
		rangeObj = t.BeforeImages
	}else if t.Operation == avro.OperationUPDATE {
		rangeObj = t.AfterImages
	}

	for _,i := range rangeObj.ArrayUnionNullIntegerCharacterDecimalFloatTimestampDateTimeTimestampWithTimeZoneBinaryGeometryTextGeometryBinaryObjectTextObjectEmptyObject{
		//fmt.Printf("%+v\n",i)
		//fmt.Printf("i type is %T\n",i)
		//fmt.Printf("Null type is %+v \n",i.Null)
		//fmt.Printf("Null value is %+v \n",i.Null)
		//fmt.Printf("Integer type is %T \n",i.Integer)
		//fmt.Printf("Integer value is %+v \n",i.Integer)
		if i.Null != nil {
			e_zaplogger.Info("i.NUll value is:",zap.Any("i.Null",i.Null))
		}else if i.Integer != nil {
			e_zaplogger.Info("i.Integer",zap.Any("i.Integer Precision is:",i.Integer.Precision),zap.Any("i.Integer Value is:",i.Integer.Value))
			columnValueList = append(columnValueList,i.Integer.Value)
		}else if i.Character != nil {
			e_zaplogger.Info("i.Character",zap.Any("i.Character Charset is:",i.Character.Charset),zap.Any("i.Character Value is:",string(i.Character.Value)))
			columnValueList = append(columnValueList,string(i.Character.Value))
		}else if i.Decimal != nil {
			e_zaplogger.Info("i.Decimal",zap.Any("i.Decimal Precision is:",i.Decimal.Precision),zap.Any("i.Decimal Scale is:",i.Decimal.Scale),zap.Any("i.Decimal Value is:",i.Decimal.Value))
			columnValueList = append(columnValueList,i.Decimal.Value)
		} else if i.Float !=nil {
			e_zaplogger.Info("I.Float",zap.Any("I.Float Precision is:",i.Float.Precision),zap.Any("i.Float Scale is:",i.Float.Scale),zap.Any("i.Float Value is:",i.Float.Value))
			columnValueList = append(columnValueList,i.Float.Value)
		} else if i.Timestamp != nil {
			e_zaplogger.Info("i.Timestamp",zap.Any("i.Timestamp Timestamp is:",i.Timestamp.Timestamp),zap.Any("i.Timestamp Millis is:",i.Timestamp.Millis))
			columnValueList = append(columnValueList,utils.TimeStampToStr(i.Timestamp.Timestamp))
		}else if i.DateTime != nil {
			e_zaplogger.Info("i.DateTime",zap.Any("i.DateTime Year is:",i.DateTime.Year.Int),zap.Any("i.Datetime Month is:",i.DateTime.Month.Int),zap.Any("i.DateTime Day is:",i.DateTime.Day.Int),
				zap.Any("i.DateTime Hour is:",i.DateTime.Hour.Int),zap.Any("i.DateTime Minute is:",i.DateTime.Minute.Int),zap.Any("i.DateTime Seconds is:",i.DateTime.Second.Int))
			timeStr:= utils.TimeColumnSplice(i.DateTime.Year.Int,i.DateTime.Month.Int,i.DateTime.Day.Int,i.DateTime.Hour.Int,i.DateTime.Minute.Int,i.DateTime.Second.Int)
			columnValueList = append(columnValueList,timeStr)
		}else if i.TimestampWithTimeZone != nil {
			e_zaplogger.Info("i.TimestampWithTimeZone",zap.Any("i.TimestampWithTimeZone Value is:",i.TimestampWithTimeZone.Value),zap.Any("i.TimestampWithTimeZone Zone is:",i.TimestampWithTimeZone.Timezone))
			columnValueList = append(columnValueList,utils.DateTimeToStr(i.TimestampWithTimeZone.Value))
		}else if i.BinaryGeometry != nil {
			e_zaplogger.Info("i.BinaryGeometry",zap.Any("i.BinaryGeometry Type is: ",i.BinaryGeometry.Type),zap.Any("i.BinaryGeometry Value is:",string(i.BinaryGeometry.Value)))
			columnValueList = append(columnValueList,string(i.BinaryGeometry.Value))
		}else if i.TextGeometry != nil {
			e_zaplogger.Info("i.TextGeometry",zap.Any("i.TextGeometry Type is:",i.TextGeometry.Type),zap.Any("i.TextGeometry Value is:",i.TextGeometry.Value))
			columnValueList = append(columnValueList,i.TextGeometry.Value)
		}else if i.BinaryObject != nil {
			e_zaplogger.Info("i.BinaryObject",zap.Any("i.BinaryObject Type is:",i.BinaryObject.Type),zap.Any("i.BinaryObject Value is:",string(i.BinaryObject.Value)))
			columnValueList = append(columnValueList,string(i.BinaryObject.Value))
		}else if i.TextObject != nil {
			e_zaplogger.Info("i.TextObject",zap.Any("i.TextObject Type is:",i.TextObject.Type),zap.Any("i.TextObject Value is:",i.TextObject.Value))
			columnValueList = append(columnValueList,i.TextObject.Value)
		}else if i.EmptyObject.String() == "NULL"  {
			columnValueList = append(columnValueList,"NULL")
		}else if i.EmptyObject.String() == "NONE" {
			columnValueList = append(columnValueList,"NONE")
		}
	}
	return columnMapList,columnValueList
}


func (k *KafkaConsumerItem)KafkaConsumerProcess(kafkaUser,kafkaPassword string,kafkaTopic []string, kafkaGroupId string,kafkaBrokerList []string,dbConn db.DbConnObj,e_zaplogger *zap.Logger)  {
	var (
		r io.Reader
		ReordMap map[string]interface{}
		consumer *cluster.Consumer
		err error
	)

	//OpMap
	opMapObj := utils.NewOpMapObj("","")

	//inint kafka consumer cluster client
	clusterconfig := cluster.NewConfig()
	clusterconfig.Consumer.Return.Errors = true
	clusterconfig.Group.Return.Notifications = true
	clusterconfig.Net.MaxOpenRequests = 100
	clusterconfig.Consumer.Offsets.AutoCommit.Enable = false
	//clusterconfig.Consumer.Offsets.Initial = sarama.OffsetNewest
	clusterconfig.Consumer.Offsets.Initial = sarama.OffsetOldest
	clusterconfig.Net.SASL.Enable = true
	clusterconfig.Net.SASL.User = kafkaUser + "-" + kafkaGroupId
	clusterconfig.Net.SASL.Password = kafkaPassword
	clusterconfig.Version = sarama.V0_11_0_0
	e_zaplogger.Info("KafkaConsumerProcess...:",zap.Any("kafkaBrokerList",kafkaBrokerList),zap.Any("kafkaGroupId",kafkaGroupId),zap.Any("kafkaTopic",kafkaTopic))
	consumer, err = cluster.NewConsumer(kafkaBrokerList, kafkaGroupId, kafkaTopic, clusterconfig)
	if err != nil {
		//retry op....
		time.Sleep(5 * time.Second)
		consumer, err = cluster.NewConsumer(kafkaBrokerList,kafkaGroupId,kafkaTopic,clusterconfig)
		if err != nil {
			panic(err)
		}


	}
	defer consumer.Close()
	// trap SIGINT to trigger a shutdown.
	signal.Notify(k.ExitConsumerSignal, os.Kill)

	// consume errors
	go func() {
		for err := range consumer.Errors() {
			//panic(err)
			e_zaplogger.Error("consumer Error：",zap.Any("error",err))
		}
	}()

	// consume notifications
	go func() {
		for ntf := range consumer.Notifications() {
			e_zaplogger.Info("Rebalanced Notifications :",zap.Any("ntf",ntf))
		}
	}()


	// Pre compile schema of avro
	t := avro.NewRecord()
	//fmt.Println(t.Schema())
	deser, err := compiler.CompileSchemaBytes([]byte(t.Schema()), []byte(t.Schema()))
	if err != nil {
		panic(err)
	}

	for {
	Loop:
		select {
		case msg, ok := <-consumer.Messages():
			if ok {
				//fmt.Fprintf(os.Stdout, "%s/%d/%d\t%+v\t%+v\n", msg.Topic, msg.Partition, msg.Offset, msg.Key,msg.Value)
				r = bytes.NewReader(msg.Value)
				t = avro.NewRecord()
				if err = vm.Eval(r, deser, t); err != nil {
					e_zaplogger.Error("vm.Eval Error",zap.Any("error",err))
					panic(err)
				}

				if t.Operation == avro.OperationHEARTBEAT || t.Operation == avro.OperationBEGIN || t.Operation == avro.OperationCOMMIT{
					break Loop
				} else {
					switch  {
					case t.Operation == avro.OperationINSERT:
						e_zaplogger.Debug("-------------in start OperationINSERT--------")
						strlist := strings.Split(t.ObjectName.String,".")
						dbName := strlist[0]
						tableName := strlist[1]
						if tableName == k.MonitorTableItem {
							columnMapList,columnValueList :=GetColumnDetail(t,e_zaplogger)
							columnItemsMap,_ := utils.GetMysqlColItems(columnMapList,columnValueList)

							ReordMap = make(map[string]interface{},0)
							ReordMap["dbtablename"] = t.ObjectName.String
							ReordMap["operation"] = t.Operation.String()
							ReordMap["binlogitems"] = columnItemsMap

							sql,values :=utils.GenExeSQL(opMapObj,ReordMap)
							e_zaplogger.Info("utils.GenExeSQL",zap.Any("sql",sql),zap.Any("values",values),
								zap.Any("dbName",dbName),zap.Any("tableName",tableName))

							// ToDo SQL EXEC
							if err = dbConn.Exec(sql,values...);err != nil {
								logger.Error("Exec Error",zap.Any("error",err))
							}
						}
						e_zaplogger.Debug("-------------in end OperationINSERT--------")

					case t.Operation == avro.OperationUPDATE:
						e_zaplogger.Debug("-------------in start OperationUPDATE--------")
						//fmt.Printf("t is: %+v \n",t)
						//fmt.Printf("t is: %+v \n",t.Source)
						//fmt.Printf("t is: %+v \n",t.ObjectName)
						//fmt.Printf("t is: %+v \n",t.Fields)
						//fmt.Printf("t is: %+v \n",t.BeforeImages)
						//fmt.Printf("t is: %+v \n",t.AfterImages)
						strlist := strings.Split(t.ObjectName.String,".")
						dbName := strlist[0]
						tableName := strlist[1]
						if tableName == k.MonitorTableItem {
							columnMapList, columnValueList := GetColumnDetail(t,e_zaplogger)
							columnItemsMap, _ := utils.GetMysqlColItems(columnMapList, columnValueList)

							ReordMap = make(map[string]interface{}, 0)
							ReordMap["dbtablename"] = t.ObjectName.String
							ReordMap["operation"] = t.Operation.String()
							ReordMap["binlogitems"] = columnItemsMap

							sql, values := utils.GenExeSQL(opMapObj, ReordMap)
							e_zaplogger.Info("utils.GenExeSQL",zap.Any("sql",sql),zap.Any("values",values),
								zap.Any("dbName",dbName),zap.Any("tableName",tableName))
							// ToDo SQL EXEC
							if err = dbConn.Exec(sql,values...);err != nil {
								e_zaplogger.Error("Exec Error",zap.Any("error",err))
							}
						}
						e_zaplogger.Debug("-------------in end OperationUPDATE--------")
					case t.Operation == avro.OperationDELETE:
						e_zaplogger.Debug("-------------in start OperationDELETE--------")
						strlist := strings.Split(t.ObjectName.String,".")
						dbName := strlist[0]
						tableName := strlist[1]
						if tableName == k.MonitorTableItem {
							columnMapList,columnValueList :=GetColumnDetail(t,e_zaplogger)
							columnItemsMap,_ := utils.GetMysqlColItems(columnMapList,columnValueList)

							ReordMap = make(map[string]interface{},0)
							ReordMap["dbtablename"] = t.ObjectName.String
							ReordMap["operation"] = t.Operation.String()
							ReordMap["binlogitems"] = columnItemsMap
							//GenSQL
							sql,values :=utils.GenExeSQL(opMapObj,ReordMap)
							e_zaplogger.Info("utils.GenExeSQL",zap.Any("sql",sql),zap.Any("values",values),
								zap.Any("dbName",dbName),zap.Any("tableName",tableName))

							// ToDo SQL EXEC
							if err = dbConn.Exec(sql);err != nil {
								e_zaplogger.Error("Exec Error",zap.Any("error",err))
							}
							e_zaplogger.Debug("-------------in end OperationDELETE--------")
						}


					case t.Operation == avro.OperationDDL:
						e_zaplogger.Debug("-------------in start OperationDDL--------")
						ddlSql := t.AfterImages.String
						e_zaplogger.Debug(ddlSql)
						//Regex
						ddlRegexStr := "(?i)" +`TABLE `+"`"+`([^\s]+)`+"`"+"|"+`TABLE `+`([^\s]+)`
						createifRegexStr := "(?i)" +`TABLE IF NOT EXISTS `+"`"+`([^\s]+)`+"`"+"|"+`TABLE IF NOT EXISTS `+`([^\s]+)`
						indexRegexStr := "(?i)" +`CREATE INDEX `+`([^\s]+)`+` ON `+"`"+`([^\s]+)`+"`"+"|"+`CREATE INDEX `+`([^\s]+)`+` ON `+`([^\s]+)`
						compile,_ := regexp.Compile(ddlRegexStr)
						submatch := compile.FindAllString(ddlSql,-1)
						createifcompile,_ := regexp.Compile(createifRegexStr)
						createifsubmatch := createifcompile.FindAllString(ddlSql,-1)
						indexcompile,_ := regexp.Compile(indexRegexStr)
						indexsubmatch := indexcompile.FindAllString(ddlSql,-1)
						if len(submatch) >0{
							match :=strings.ReplaceAll(submatch[0],"`","")
							match2 := strings.ReplaceAll(match,"("," ")
							matchlist := strings.Split(match2," ")
							tablename := matchlist[1]
							if tablename == k.MonitorTableItem {
								e_zaplogger.Info("Match ddlSql",zap.Any("ddlsql",ddlSql))
								//DEST DB EXEC
								if err = dbConn.Exec(ddlSql);err != nil {
									e_zaplogger.Error("Exec Error",zap.Any("error",err))
								}
							}
						}
						if len(createifsubmatch) > 0 {
							match :=strings.ReplaceAll(createifsubmatch[0],"`","")
							match2 := strings.ReplaceAll(match,"("," ")
							matchlist := strings.Split(match2," ")
							tablename := matchlist[4]
							if tablename == k.MonitorTableItem {
								e_zaplogger.Info("Match ddlSql",zap.Any("ddlsql",ddlSql))
								//DEST DB EXEC
								if err = dbConn.Exec(ddlSql);err != nil {
									e_zaplogger.Error("Exec Error",zap.Any("error",err))
								}
							}
						}
						if len(indexsubmatch) >0 {
							match :=strings.ReplaceAll(indexsubmatch[0],"`","")
							match2 := strings.ReplaceAll(match,"("," ")
							matchlist := strings.Split(match2,"ON")
							tablename := strings.TrimSpace(matchlist[1])
							if tablename == k.MonitorTableItem {
								e_zaplogger.Info("Match ddlSql",zap.Any("ddlsql",ddlSql))
								//DEST DB EXEC
								if err = dbConn.Exec(ddlSql);err != nil {
									e_zaplogger.Error("Exec Error",zap.Any("error",err))
								}
							}

						}

						e_zaplogger.Debug("-------------in end OperationDDL--------")

					}
				}

				//Manually confirm kafka message
				consumer.MarkOffset(msg,"")
				//consumer.CommitOffsets()
			}
		case <-k.ExitConsumerSignal:
			e_zaplogger.Sync()
			logger.Info("################@@@@@@@@@ Consumer Quit.......##############")
			return
		}
	}

}
