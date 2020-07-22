package config

import (
	"fmt"
	"github.com/go-ini/ini"
)

type APPConfig struct {
	*MySQLConfig `json:"mysql" ini:"mysql"`
	*LogConfig `json:"log" ini:"log"`
}

type MySQLConfig struct {
	Host string `json:"host" ini:"host"`
	Username string `json:"username" ini:"username"`
	Password string `json:"password" ini:"password"`
	Port string `json:"port" ini:"port"`
	DB string `json:"db" ini:"db"`
}

type LogConfig struct {
	Level string `json:"level" ini:"level"`
	LogPath string `json:"logpath" ini:"logpath"`
	MaxSize uint `json:"maxsize" ini:"maxsize"`
	MaxAge uint `json:"maxage" ini:"maxage"`
	MaxBackups uint `json:"maxbackups" ini:"maxbackups"`
}


var Conf = new(APPConfig)

func InitFromIni(filename string) (err error) {
	err = ini.MapTo(Conf,filename)
	if err != nil {
		panic(fmt.Sprintf("Init conf file failed %s",err))
	}
	return err
}