package client

import (
	"sale/pkg/authctx"
	userv1 "user/api/user/v1"

	"github.com/cloudwego/hertz/pkg/common/hlog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var userClient userv1.UserClient

func initUserGrpc() {
	conn, err := grpc.NewClient("127.0.0.1:9000",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithUnaryInterceptor(authctx.ClientUnaryInterceptor()), // 新增
		// 出站注入
		// grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
	)
	if err != nil {
		// 调用失败
		hlog.Error("调用user服务失败")
		return
	}
	userClient = userv1.NewUserClient(conn)
}

func GetUserClient() userv1.UserClient {
	return userClient
}
