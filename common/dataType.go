package common

type DataType int32

const (
	MYSQL_TYPE_DECIMAL       DataType = 0
	MYSQL_TYPE_INT8          DataType = 1
	MYSQL_TYPE_INT16         DataType = 2
	MYSQL_TYPE_INT32         DataType = 3
	MYSQL_TYPE_FLOAT         DataType = 4
	MYSQL_TYPE_DOUBLE        DataType = 5
	MYSQL_TYPE_NULL          DataType = 6
	MYSQL_TYPE_TIMESTAMP     DataType = 7
	MYSQL_TYPE_INT64         DataType = 8
	MYSQL_TYPE_INT24         DataType = 9
	MYSQL_TYPE_DATE          DataType = 10
	MYSQL_TYPE_TIME          DataType = 11
	MYSQL_TYPE_DATETIME      DataType = 12
	MYSQL_TYPE_YEAR          DataType = 13
	MYSQL_TYPE_DATE_NEW      DataType = 14
	MYSQL_TYPE_VARCHAR       DataType = 15
	MYSQL_TYPE_BIT           DataType = 16
	MYSQL_TYPE_TIMESTAMP_NEW DataType = 17
	MYSQL_TYPE_DATETIME_NEW  DataType = 18
	MYSQL_TYPE_TIME_NEW      DataType = 19
	MYSQL_TYPE_JSON          DataType = 245
	MYSQL_TYPE_DECIMAL_NEW   DataType = 246
	MYSQL_TYPE_ENUM          DataType = 247
	MYSQL_TYPE_SET           DataType = 248
	MYSQL_TYPE_TINY_BLOB     DataType = 249
	MYSQL_TYPE_MEDIUM_BLOB   DataType = 250
	MYSQL_TYPE_LONG_BLOB     DataType = 251
	MYSQL_TYPE_BLOB          DataType = 252
	MYSQL_TYPE_VAR_STRING    DataType = 253
	MYSQL_TYPE_STRING        DataType = 254
	MYSQL_TYPE_GEOMETRY      DataType = 255
)

func GetdataTypeName(datatypenum int32) string  {
	switch datatypenum {
	case 0:
		return "MYSQL_TYPE_DECIMAL"
	case 1:
		return "MYSQL_TYPE_INT8"
	case 2:
		return "MYSQL_TYPE_INT16"
	case 3:
		return "MYSQL_TYPE_INT32"
	case 4:
		return "MYSQL_TYPE_FLOAT"
	case 5:
		return "MYSQL_TYPE_DOUBLE"
	case 6:
		return "MYSQL_TYPE_NULL"
	case 7:
		return "MYSQL_TYPE_TIMESTAMP"
	case 8:
		return "MYSQL_TYPE_INT64"
	case 9:
		return "MYSQL_TYPE_INT24"
	case 10:
		return "MYSQL_TYPE_DATE"
	case 11:
		return "MYSQL_TYPE_TIME"
	case 12:
		return "MYSQL_TYPE_DATETIME"
	case 13:
		return "MYSQL_TYPE_YEAR"
	case 14:
		return "MYSQL_TYPE_DATE_NEW"
	case 15:
		return "MYSQL_TYPE_VARCHAR"
	case 16:
		return "MYSQL_TYPE_BIT"
	case 17:
		return "MYSQL_TYPE_TIMESTAMP_NEW"
	case 18:
		return "MYSQL_TYPE_DATETIME_NEW"
	case 19:
		return "MYSQL_TYPE_TIME_NEW"
	case 245:
		return "MYSQL_TYPE_JSON"
	case 246:
		return "MYSQL_TYPE_DECIMAL_NEW"
	case 247:
		return "MYSQL_TYPE_ENUM"
	case 248:
		return "MYSQL_TYPE_SET"
	case 249:
		return "MYSQL_TYPE_TINY_BLOB"
	case 250:
		return "MYSQL_TYPE_MEDIUM_BLOB"
	case 251:
		return "MYSQL_TYPE_LONG_BLOB"
	case 252:
		return "MYSQL_TYPE_BLOB"
	case 253:
		return "MYSQL_TYPE_VAR_STRING"
	case 254:
		return "MYSQL_TYPE_STRING"
	case 255:
		return "MYSQL_TYPE_GEOMETRY"

	}
	return ""
}