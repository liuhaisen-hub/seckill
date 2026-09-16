package server

import (
	"log/slog"

	eventv1 "activity/api/event/v1"
	promov1 "activity/api/promo/v1"
	showv1 "activity/api/show/v1"
	statsv1 "activity/api/stats/v1"
	tickettypev1 "activity/api/ticket_type/v1"
	transferv1 "activity/api/transfer/v1"
	"activity/internal/service"
	"sale/pkg/authctx"
	"sale/pkg/conf"

	"github.com/go-kratos/kratos/v3/middleware/logging"
	"github.com/go-kratos/kratos/v3/middleware/recovery"
	"github.com/go-kratos/kratos/v3/transport/grpc"
)

// NewGRPCServer new a gRPC server.
func NewGRPCServer(
	c *conf.Server,
	logger *slog.Logger,
	eventSvc *service.EventService,
	promoSvc *service.PromoService,
	showSvc *service.ShowService,
	statsSvc *service.StatsService,
	ticketTypeSvc *service.TicketTypeService,
	transferSvc *service.TransferService,
) *grpc.Server {
	var opts = []grpc.ServerOption{
		grpc.Middleware(
			recovery.Recovery(),
			logging.Server(logger),
			authctx.ContextMiddleware(),
		),
	}
	if c.Grpc.Network != "" {
		opts = append(opts, grpc.Network(c.Grpc.Network))
	}
	if c.Grpc.Addr != "" {
		opts = append(opts, grpc.Address(c.Grpc.Addr))
	}
	if c.Grpc.Timeout != nil {
		opts = append(opts, grpc.Timeout(c.Grpc.Timeout.AsDuration()))
	}
	srv := grpc.NewServer(opts...)
	eventv1.RegisterEventServer(srv, eventSvc)
	promov1.RegisterPromoServer(srv, promoSvc)
	showv1.RegisterShowServer(srv, showSvc)
	statsv1.RegisterStatsServer(srv, statsSvc)
	tickettypev1.RegisterTicketTypeServer(srv, ticketTypeSvc)
	transferv1.RegisterTransferServer(srv, transferSvc)
	return srv
}
