package utils

import (
	"fmt"
	"strconv"
	"strings"
)

func GetMysqlColItems(columnMapList []map[string]interface{},columnValueList []interface{}) (ColItemsMaps []map[string]interface{}, err error)  {

	for i,v := range columnValueList{
		//fmt.Printf("v type is: %T v value is: %T\n",v,v)
		tmpmap :=columnMapList[i]
		//fmt.Println("colName:",tmpmap["colName"])
		//tmpmap["colValue"] = v

		switch tmpmap["colType"] {
		case "MYSQL_TYPE_DECIMAL":
			tmpmap["colValue"] = v.(float64)
		case "MYSQL_TYPE_INT8":
			tmpmap["colValue"] =  v.(int8)
		case "MYSQL_TYPE_INT16":
			tmpmap["colValue"] = v.(int16)
		case "MYSQL_TYPE_INT32":
			var val string
			switch v.(type) {
			case string:
				val = v.(string)

			}
			int32Val,_ :=strconv.ParseInt(val,10,32)
			tmpmap["colValue"] =  int32(int32Val)
		case "MYSQL_TYPE_FLOAT":
			tmpmap["colValue"] = v.(float32)
		case "MYSQL_TYPE_DOUBLE":
			tmpmap["colValue"] = v.(float64)
		case "MYSQL_TYPE_NULL":
			tmpmap["colValue"] = v
		case "MYSQL_TYPE_TIMESTAMP":
			tmpmap["colValue"] = v.(string)
		case "MYSQL_TYPE_INT64":
			tmpmap["colValue"] = v.(int64)
		case "MYSQL_TYPE_INT24":
			tmpmap["colValue"] = v.(int32)
		case "MYSQL_TYPE_DATE":
			tmpmap["colValue"] = v.(string)
		case "MYSQL_TYPE_TIME":
			tmpmap["colValue"] = v.(string)
		case "MYSQL_TYPE_DATETIME":
			tmpmap["colValue"] =v.(string)
		case "MYSQL_TYPE_YEAR":
			tmpmap["colValue"] = v.(string)
		case "MYSQL_TYPE_DATE_NEW":
			tmpmap["colValue"] = v.(string)
		case "MYSQL_TYPE_VARCHAR":
			tmpmap["colValue"] = v.(string)
		case "MYSQL_TYPE_BIT":
			tmpmap["colValue"] = v.(byte)
		case "MYSQL_TYPE_TIMESTAMP_NEW":
			tmpmap["colValue"] = v.(string)
		case "MYSQL_TYPE_DATETIME_NEW":
			tmpmap["colValue"] = v.(string)
		case "MYSQL_TYPE_TIME_NEW":
			tmpmap["colValue"] = v.(string)
		case "MYSQL_TYPE_JSON":
			tmpmap["colValue"] = v.(string)
		case "MYSQL_TYPE_DECIMAL_NEW":
			tmpmap["colValue"] = v.(float64)
		case "MYSQL_TYPE_ENUM":
			tmpmap["colValue"] =v
		case "MYSQL_TYPE_SET":
			tmpmap["colValue"] =v
		case "MYSQL_TYPE_TINY_BLOB":
			tmpmap["colValue"] = v.(byte)
		case "MYSQL_TYPE_MEDIUM_BLOB":
			tmpmap["colValue"] = v.(byte)
		case "MYSQL_TYPE_LONG_BLOB":
			tmpmap["colValue"] = v.(byte)
		case "MYSQL_TYPE_BLOB":
			tmpmap["colValue"] = v.(byte)
		case "MYSQL_TYPE_VAR_STRING":
			tmpmap["colValue"] = v.(string)
		case "MYSQL_TYPE_STRING":
			tmpmap["colValue"] = v.(string)
		case "MYSQL_TYPE_GEOMETRY":
			tmpmap["colValue"] = v.(byte)

		}
		ColItemsMaps = append(ColItemsMaps,tmpmap)
	}
	return

}


type OpMap struct {
	OpName string
	OpSql string
}

func NewOpMapObj(opname,opsql string) *OpMap  {
	return &OpMap{
		OpName: opname,
		OpSql: opsql,
	}
}

func (o *OpMap) SetOpMap(op string)  {
	if op == "INSERT"{
		o.OpName = "INSERT"
		o.OpSql = "REPLACE INTO `%s`.`%s` (%s) VALUES(%s);"
	}else if op == "UPDATE" {
		o.OpName = "UPDATE"
		o.OpSql = "REPLACE INTO `%s`.`%s` (%s) VALUES(%s);"
	}else if op == "DELETE"{
		o.OpName = "DELETE"
		o.OpSql = "DELETE FROM `%s`.`%s` WHERE %s LIMIT 1;"
	}
}


func GenExeSQL(obj *OpMap,reordmap map[string]interface{}) (sql string,values []interface{}) {
	var (
		strlist []string
		dbName,tableName string
		OpSql,whereCondCol,whereCondType string
		whereCondVal interface{}
	)

	strlist = strings.Split(reordmap["dbtablename"].(string),".")
	dbName = strlist[0]
	tableName = strlist[1]
	obj.SetOpMap(reordmap["operation"].(string))
	itemsmapList := reordmap["binlogitems"].([]map[string]interface{})

	tableColumns := make([]string,0)
	tableValues := make([]interface{},0)
	for _,item := range itemsmapList{
		tableColumns = append(tableColumns,item["colName"].(string))
		tableValues = append(tableValues,item["colValue"])
	}

	columnsStr := strings.Join(tableColumns,",")

	if obj.OpName == "INSERT" || obj.OpName == "UPDATE" {
		paramsValues := make([]string,0)
		for i:=0;i<len(tableValues);i++ {
			paramsValues = append(paramsValues,"?")
		}
		paramsStr := strings.Join(paramsValues,",")
		OpSql = fmt.Sprintf(obj.OpSql,dbName,tableName,columnsStr,paramsStr)
	}else if obj.OpName == "DELETE" {
		for _,item := range itemsmapList{
			if item["colName"] == "id"{
				whereCondCol = "id"
				whereCondType = item["colType"].(string)
				whereCondVal = item["colValue"]
			}

		}
		templist := strings.Split(whereCondType,"_")
		valType := templist[len(templist)-1]
		switch valType {
		case "INT32":
			val :=whereCondVal.(int32)
			whereStatement :=fmt.Sprintf("%s=%d",whereCondCol,val)
			OpSql = fmt.Sprintf(obj.OpSql,dbName,tableName,whereStatement)
		}

	}

	return OpSql,tableValues
}
