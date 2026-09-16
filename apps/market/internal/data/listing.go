package data

import (
	"context"
	"errors"
	"log/slog"
	"sale/model"
	"sale/pkg/utils"

	"market/internal/biz"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type listingRepo struct {
	data   *Data
	logger *slog.Logger
}

func NewListingRepo(data *Data, logger *slog.Logger) biz.ListingRepo {
	return &listingRepo{
		data:   data,
		logger: logger,
	}
}

func (r *listingRepo) CreateListing(ctx context.Context, listing *model.MarketplaceListing) (*model.MarketplaceListing, error) {
	if err := r.data.db.WithContext(ctx).Model(&model.MarketplaceListing{}).Create(listing).Error; err != nil {
		return nil, err
	}
	return listing, nil
}

func (r *listingRepo) GetListing(ctx context.Context, id int64) (*model.MarketplaceListing, error) {
	var listing model.MarketplaceListing
	err := r.data.db.WithContext(ctx).Where("id = ?", id).First(&listing).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, biz.ErrListingNotFound
	}
	if err != nil {
		return nil, err
	}
	return &listing, nil
}

func (r *listingRepo) GetActiveListingByTicket(ctx context.Context, ticketID int64) (*model.MarketplaceListing, error) {
	var listing model.MarketplaceListing
	err := r.data.db.WithContext(ctx).
		Where("ticket_id = ? AND status = ?", ticketID, biz.ListingStatusActive).
		First(&listing).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		// 没有在售挂牌不算错误，由 biz 判断是否允许上架
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &listing, nil
}

func (r *listingRepo) ListListings(ctx context.Context, opts ...utils.ListOption) ([]*model.MarketplaceListing, int64, error) {
	o := utils.NewListOptions(opts...)

	db := r.data.db.WithContext(ctx).Model(&model.MarketplaceListing{})
	for field, value := range o.Filters {
		db = db.Where(field+" = ?", value)
	}
	for _, order := range o.OrderBy {
		db = db.Order(order)
	}

	var total int64
	if err := db.Count(&total).Error; err != nil {
		return nil, 0, err
	}

	var list []*model.MarketplaceListing
	if err := db.Offset(o.Offset).Limit(o.Limit).Find(&list).Error; err != nil {
		return nil, 0, err
	}
	return list, total, nil
}

func (r *listingRepo) ListActiveByEvent(ctx context.Context, eventID int64, opts ...utils.ListOption) ([]*model.MarketplaceListing, int64, error) {
	o := utils.NewListOptions(opts...)

	// 挂牌表没有 event_id，通过票的冗余字段联表过滤
	db := r.data.db.WithContext(ctx).Model(&model.MarketplaceListing{}).
		Joins("JOIN tickets ON tickets.id = marketplace_listings.ticket_id").
		Where("tickets.event_id = ? AND marketplace_listings.status = ?", eventID, biz.ListingStatusActive)
	for field, value := range o.Filters {
		db = db.Where("marketplace_listings."+field+" = ?", value)
	}
	for _, order := range o.OrderBy {
		db = db.Order(order)
	}

	var total int64
	if err := db.Count(&total).Error; err != nil {
		return nil, 0, err
	}

	var list []*model.MarketplaceListing
	if err := db.Offset(o.Offset).Limit(o.Limit).Find(&list).Error; err != nil {
		return nil, 0, err
	}
	return list, total, nil
}

func (r *listingRepo) UpdateListing(ctx context.Context, listing *model.MarketplaceListing) error {
	return r.data.db.WithContext(ctx).Save(listing).Error
}

func (r *listingRepo) BuyListing(ctx context.Context, listingID, buyerID int64) error {
	// 事务 + 行锁：并发购买同一挂牌时，后到的事务会看到 status 已变为 sold 而失败。
	return r.data.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var listing model.MarketplaceListing
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("id = ?", listingID).First(&listing).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return biz.ErrListingNotFound
			}
			return err
		}
		if listing.Status != biz.ListingStatusActive {
			return biz.ErrListingNotActive
		}
		if listing.SellerID == uint(buyerID) {
			return biz.ErrBuyOwnListing
		}
		// 行锁查询票务，防止过户期间票被其他流程改动
		var ticket model.Ticket
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("id = ?", listing.TicketID).First(&ticket).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return biz.ErrTicketNotFound
			}
			return err
		}
		if ticket.UserID != listing.SellerID {
			return biz.ErrTicketMismatch
		}
		// 转移票务所有权
		if err := tx.Model(&model.Ticket{}).Where("id = ?", ticket.ID).
			Updates(map[string]any{
				"user_id":         buyerID,
				"transfer_status": biz.TransferStatusApproved,
				"transferred_to":  buyerID,
			}).Error; err != nil {
			return err
		}
		// 更新挂牌为已售出
		return tx.Model(&model.MarketplaceListing{}).Where("id = ?", listing.ID).
			Updates(map[string]any{
				"status":   biz.ListingStatusSold,
				"buyer_id": buyerID,
			}).Error
	})
}
