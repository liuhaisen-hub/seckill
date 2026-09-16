package client

import (
	"sale/pkg/conf"
	"sale/pkg/rdb"

	"github.com/redis/go-redis/v9"
)

var rdbClient *redis.Client

func NewRdb(cfg *conf.Data) {
	rdc := cfg.GetRedis()
	rdbClient = rdb.NewRdbClient(rdc)
}

func GetRedisClient() *redis.Client {
	return rdbClient
}
