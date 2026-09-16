package data

import (
	"context"
	"log/slog"
	"sale/model"

	"market/internal/biz"
)

type catalogRepo struct {
	data   *Data
	logger *slog.Logger
}

func NewCatalogRepo(data *Data, logger *slog.Logger) biz.CatalogRepo {
	return &catalogRepo{
		data:   data,
		logger: logger,
	}
}

func (r *catalogRepo) GetTicket(ctx context.Context, id int64) (*model.Ticket, error) {
	var ticket model.Ticket
	if err := r.data.db.WithContext(ctx).Where("id = ?", id).First(&ticket).Error; err != nil {
		return nil, err
	}
	return &ticket, nil
}

func (r *catalogRepo) GetTicketsByIDs(ctx context.Context, ids []int64) ([]*model.Ticket, error) {
	var tickets []*model.Ticket
	if err := r.data.db.WithContext(ctx).Where("id IN ?", ids).Find(&tickets).Error; err != nil {
		return nil, err
	}
	return tickets, nil
}

func (r *catalogRepo) GetTicketTypesByIDs(ctx context.Context, ids []int64) ([]*model.TicketType, error) {
	var tts []*model.TicketType
	if err := r.data.db.WithContext(ctx).Where("id IN ?", ids).Find(&tts).Error; err != nil {
		return nil, err
	}
	return tts, nil
}

func (r *catalogRepo) GetEventsByIDs(ctx context.Context, ids []int64) ([]*model.Event, error) {
	var events []*model.Event
	if err := r.data.db.WithContext(ctx).Where("id IN ?", ids).Find(&events).Error; err != nil {
		return nil, err
	}
	return events, nil
}
