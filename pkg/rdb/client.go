package rdb

import (
	"net"
	"sale/pkg/conf"
	"strconv"

	"github.com/redis/go-redis/v9"
)

// 封装redis 客户端

func NewRdbClient(dc *conf.Data_Redis) *redis.Client {
	addr := net.JoinHostPort(dc.GetHost(), strconv.Itoa(int(dc.Port)))
	client := redis.NewClient(&redis.Options{
		Addr:         addr,
		Password:     dc.GetPassword(),
		DB:           int(dc.GetDb()),
		ReadTimeout:  dc.GetReadTimeout().AsDuration(),
		WriteTimeout: dc.GetWriteTimeout().AsDuration(),
	})
	return client
}
