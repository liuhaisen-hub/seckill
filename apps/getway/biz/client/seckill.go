package client

import (
	"sale/pkg/authctx"
	seckillv1 "seckill/api/seckill/v1"

	"github.com/cloudwego/hertz/pkg/common/hlog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var seckillRpc seckillv1.SeckillClient

func newSeckillClient() {
	conn, err := grpc.NewClient("127.0.0.1:9003",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithUnaryInterceptor(authctx.ClientUnaryInterceptor()), // 新增
		// // 出站注入
		// grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
	)
	if err != nil {
		hlog.Error(err.Error())
	}
	seckillRpc = seckillv1.NewSeckillClient(conn)
}

func GetSeckillClient() seckillv1.SeckillClient {
	return seckillRpc
}
