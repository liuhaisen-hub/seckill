package client

import (
	eventv1 "activity/api/event/v1"
	promov1 "activity/api/promo/v1"
	showv1 "activity/api/show/v1"
	statsv1 "activity/api/stats/v1"
	tickettypev1 "activity/api/ticket_type/v1"
	transferv1 "activity/api/transfer/v1"
	"sale/pkg/authctx"

	"github.com/cloudwego/hertz/pkg/common/hlog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type activityRpc struct {
	eventClient     eventv1.EventClient
	promoClient     promov1.PromoClient
	showClient      showv1.ShowClient
	statsClient     statsv1.StatsClient
	ticketTypeCient tickettypev1.TicketTypeClient
	transferClient  transferv1.TransferClient
}

var adminClient *activityRpc

func newActivityRpc() {
	conn, err := grpc.NewClient("127.0.0.1:9001",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithUnaryInterceptor(authctx.ClientUnaryInterceptor()), // 新增
		// // 出站注入
		// grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
	)
	if err != nil {
		hlog.Error(err.Error())
	}
	event := eventv1.NewEventClient(conn)
	promo := promov1.NewPromoClient(conn)
	show := showv1.NewShowClient(conn)
	stats := statsv1.NewStatsClient(conn)
	ticketType := tickettypev1.NewTicketTypeClient(conn)
	transfer := transferv1.NewTransferClient(conn)
	adminClient = &activityRpc{
		eventClient:     event,
		promoClient:     promo,
		showClient:      show,
		statsClient:     stats,
		ticketTypeCient: ticketType,
		transferClient:  transfer,
	}
}

func GetEvent() eventv1.EventClient {
	return adminClient.eventClient
}

func GetPromo() promov1.PromoClient {
	return adminClient.promoClient
}

func GetShow() showv1.ShowClient {
	return adminClient.showClient
}

func GetStats() statsv1.StatsClient {
	return adminClient.statsClient
}
func GetTicketType() tickettypev1.TicketTypeClient {
	return adminClient.ticketTypeCient
}

func GetTransfer() transferv1.TransferClient {
	return adminClient.transferClient
}
