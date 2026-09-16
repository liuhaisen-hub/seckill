package client

import (
	marketv1 "market/api/market/v1"
	"sale/pkg/authctx"

	"github.com/cloudwego/hertz/pkg/common/hlog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type marketRpc struct {
	marketClient marketv1.MarketplaceClient
}

var mktClient *marketRpc

func newMarketRpc() {
	conn, err := grpc.NewClient("127.0.0.1:9003",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithUnaryInterceptor(authctx.ClientUnaryInterceptor()), // 新增
		// // 出站注入
		// grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
	)
	if err != nil {
		hlog.Error(err.Error())
	}
	market := marketv1.NewMarketplaceClient(conn)
	mktClient = &marketRpc{
		marketClient: market,
	}
}

func GetMarket() marketv1.MarketplaceClient {
	return mktClient.marketClient
}
