package service

import "github.com/google/wire"

// ProviderSet is service providers.
var ProviderSet = wire.NewSet(NewEventService, NewPromoService, NewShowService, NewStatsService, NewTicketTypeService, NewTransferService)
