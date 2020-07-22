package db

import (
	"github.com/go-redis/redis/v7"
	"goimpl/config"
)

const RedisDev = "dev"

type redisList struct {
	Dev *redis.Client
}


var r redisList

type RedisIns struct {
	*redis.Client
}

func DevRedis() *RedisIns {
	return &RedisIns{r.Dev}
}

func InitRedisIns(names ...string) (error)  {
	for _,name := range names{
		ins,err := initRedis(name)
		if err != nil {
			return err
		}
		switch name {
		case RedisDev:
			r.Dev = ins
		}
	}
	return nil
}

func initRedis(name string) (*redis.Client,error) {
	c := config.GetAcmConfig().GetStringMapString("database.redis."+name)
	rc := redis.NewClient(&redis.Options{
		Addr:     c["host"] + ":" + c["port"],
		Password: c["password"],
		DB:       0,
	})
	if err := rc.Ping().Err();err != nil {
		return nil, err

	}
	return rc,nil
}

func CloseRedis(names ...string)  {
	for _,name := range names{
		switch name {
		case RedisDev:
			r.Dev.Close()
		}
	}
}