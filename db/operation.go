package db

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"go.uber.org/zap"
	"goimpl/logger"
)

type DbConnObj struct {
	DbConn *sql.DB
}

var (
	DbObj DbConnObj
)

func GetDbObj() DbConnObj {
	return DbObj
}

func InitDb(dbuser,dbpassword,host,port,dbname string) (err error)  {
	DbObj := new(DbConnObj)
	dbConn,err := NewDbConn(dbuser,dbpassword,host,port,dbname)
	if err != nil {
		logger.Error("Init Db failed error:",zap.Any("error",err))
		return
	}
	DbObj.DbConn = dbConn
	return
}


func NewDbConn(dbuser,dbpassword,host,port,dbname string) (Dbconn *sql.DB,err error)  {

	dbConnStr := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?charset=utf8mb4",dbuser,dbpassword,host,port,dbname)
	Dbconn,err =  sql.Open("mysql",dbConnStr)
	if err != nil {
		logger.Error("NewDbConn occur an error:",zap.Any("error",err))
		return nil,err
	}

	return

}

func (dc *DbConnObj) Closer()  {
	if dc.DbConn != nil {
		dc.DbConn.Close()
	}
}

func (db *DbConnObj) Exec(sql string,values ...interface{})  (error) {
	result,err := db.DbConn.Exec(sql,values...)
	if err != nil {
		logger.Error("Exec SQL error:",zap.Any("error",err))
		return err
	}

	lastInsertId ,_ := result.LastInsertId()
	rowsAffected,_ := result.RowsAffected()
	logger.Info("Resulst LastInsertId:",zap.Any("LastInsertId",lastInsertId))
	logger.Info("Resulst RowsAffected:",zap.Any("RowsAffected",rowsAffected))
	//fmt.Println(result.LastInsertId())
	//fmt.Println(result.RowsAffected())
	return nil
}




