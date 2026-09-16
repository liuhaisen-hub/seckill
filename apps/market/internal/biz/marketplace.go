package biz

import (
	"context"
	"log/slog"
	"sale/model"
	"sale/pkg/utils"

	"github.com/go-kratos/kratos/v3/errors"
)

// 挂牌状态，与 model 层的默认值保持一致。
const (
	ListingStatusActive    = "active"
	ListingStatusSold      = "sold"
	ListingStatusCancelled = "cancelled"
)

// TicketStatusPaid 复用票务领域的已支付状态：只有已支付的票才能上架。
const TicketStatusPaid = "paid"

var (
	ErrListingNotFound  = errors.NotFound("LISTING_NOT_FOUND", "挂牌不存在")
	ErrTicketNotFound   = errors.NotFound("TICKET_NOT_FOUND", "票务不存在")
	ErrListingNotActive = errors.BadRequest("LISTING_NOT_ACTIVE", "该商品已下架或已售出")
	ErrBuyOwnListing    = errors.BadRequest("LISTING_FORBIDDEN", "不能购买自己的票")
	ErrTicketMismatch   = errors.BadRequest("TICKET_MISMATCH", "票务信息不一致")
)

// ListingRepo 是二手市场挂牌仓储接口，由 data 层实现，是依赖倒置的接缝。
type ListingRepo interface {
	CreateListing(ctx context.Context, listing *model.MarketplaceListing) (*model.MarketplaceListing, error)
	GetListing(ctx context.Context, id int64) (*model.MarketplaceListing, error)
	UpdateListing(ctx context.Context, listing *model.MarketplaceListing) error
	// GetActiveListingByTicket 查某张票当前的有效挂牌，不存在时返回 (nil, nil)。
	GetActiveListingByTicket(ctx context.Context, ticketID int64) (*model.MarketplaceListing, error)
	ListListings(ctx context.Context, opts ...utils.ListOption) ([]*model.MarketplaceListing, int64, error)
	// ListActiveByEvent 联表 tickets 查某活动下的在售挂牌。
	ListActiveByEvent(ctx context.Context, eventID int64, opts ...utils.ListOption) ([]*model.MarketplaceListing, int64, error)
	// BuyListing 事务内完成购买：校验、行锁、票过户、更新挂牌。
	// 业务规则依赖行锁保证并发正确，只能整体下沉到 data 层的事务里。
	BuyListing(ctx context.Context, listingID, buyerID int64) error
}

// CatalogRepo 提供挂牌关联信息（票、票种、活动）的只读查询，
// 用于组装展示数据，市场服务自己不写这些表。
type CatalogRepo interface {
	GetTicket(ctx context.Context, id int64) (*model.Ticket, error)
	GetTicketsByIDs(ctx context.Context, ids []int64) ([]*model.Ticket, error)
	GetTicketTypesByIDs(ctx context.Context, ids []int64) ([]*model.TicketType, error)
	GetEventsByIDs(ctx context.Context, ids []int64) ([]*model.Event, error)
}

// MarketplaceUseCase 是二手市场业务用例。
type MarketplaceUseCase struct {
	listingRepo ListingRepo
	catalogRepo CatalogRepo
	logger      *slog.Logger
}

func NewMarketplaceUseCase(listingRepo ListingRepo, catalogRepo CatalogRepo, logger *slog.Logger) *MarketplaceUseCase {
	return &MarketplaceUseCase{
		listingRepo: listingRepo,
		catalogRepo: catalogRepo,
		logger:      logger,
	}
}

// ListingView 是挂牌的读模型：挂牌本体 + 关联票种/活动的展示信息。
// 只组合引用不复制字段，避免再造一层结构体。
type ListingView struct {
	*model.MarketplaceListing
	EventID    uint
	EventTitle string
	TicketName string
}

func (uc *MarketplaceUseCase) CreateListing(ctx context.Context, sellerID, ticketID int64, price float64, description string) (*model.MarketplaceListing, error) {
	if sellerID == 0 {
		return nil, errors.BadRequest("LISTING_BAD_REQUEST", "卖家 ID 不能为空")
	}
	if ticketID == 0 {
		return nil, errors.BadRequest("LISTING_BAD_REQUEST", "票 ID 不能为空")
	}
	if price <= 0 {
		return nil, errors.BadRequest("LISTING_BAD_REQUEST", "价格必须大于 0")
	}
	ticket, err := uc.catalogRepo.GetTicket(ctx, ticketID)
	if err != nil {
		return nil, ErrTicketNotFound
	}
	if ticket.UserID != uint(sellerID) {
		return nil, errors.BadRequest("LISTING_FORBIDDEN", "无权出售此票务")
	}
	if ticket.Status != TicketStatusPaid {
		return nil, errors.BadRequest("LISTING_BAD_REQUEST", "只有已支付的票务才能上架")
	}
	existing, err := uc.listingRepo.GetActiveListingByTicket(ctx, ticketID)
	if err != nil {
		return nil, err
	}
	if existing != nil {
		return nil, errors.BadRequest("LISTING_CONFLICT", "该票务已在转让市场中")
	}
	return uc.listingRepo.CreateListing(ctx, &model.MarketplaceListing{
		TicketID:    uint(ticketID),
		SellerID:    uint(sellerID),
		Price:       price,
		Status:      ListingStatusActive,
		Description: description,
	})
}

func (uc *MarketplaceUseCase) BuyListing(ctx context.Context, listingID, buyerID int64) error {
	if listingID == 0 {
		return errors.BadRequest("LISTING_BAD_REQUEST", "挂牌 ID 不能为空")
	}
	if buyerID == 0 {
		return errors.BadRequest("LISTING_BAD_REQUEST", "买家 ID 不能为空")
	}
	return uc.listingRepo.BuyListing(ctx, listingID, buyerID)
}

func (uc *MarketplaceUseCase) CancelListing(ctx context.Context, listingID, sellerID int64) error {
	if listingID == 0 {
		return errors.BadRequest("LISTING_BAD_REQUEST", "挂牌 ID 不能为空")
	}
	if sellerID == 0 {
		return errors.BadRequest("LISTING_BAD_REQUEST", "卖家 ID 不能为空")
	}
	listing, err := uc.listingRepo.GetListing(ctx, listingID)
	if err != nil {
		return err
	}
	if listing.SellerID != uint(sellerID) {
		return errors.BadRequest("LISTING_FORBIDDEN", "无权取消此商品")
	}
	if listing.Status != ListingStatusActive {
		return ErrListingNotActive
	}
	listing.Status = ListingStatusCancelled
	return uc.listingRepo.UpdateListing(ctx, listing)
}

func (uc *MarketplaceUseCase) GetListing(ctx context.Context, id int64) (*ListingView, error) {
	if id == 0 {
		return nil, errors.BadRequest("LISTING_BAD_REQUEST", "挂牌 ID 不能为空")
	}
	listing, err := uc.listingRepo.GetListing(ctx, id)
	if err != nil {
		return nil, err
	}
	views, err := uc.enrichListings(ctx, []*model.MarketplaceListing{listing})
	if err != nil {
		return nil, err
	}
	return views[0], nil
}

func (uc *MarketplaceUseCase) ListActiveListings(ctx context.Context, page, size int64) ([]*ListingView, int64, error) {
	list, total, err := uc.listingRepo.ListListings(ctx, append(pageOptions(page, size),
		utils.ListFilter("status", ListingStatusActive),
		utils.ListOrderBy("created_at DESC"),
	)...)
	if err != nil {
		return nil, 0, err
	}
	views, err := uc.enrichListings(ctx, list)
	return views, total, err
}

func (uc *MarketplaceUseCase) ListEventListings(ctx context.Context, eventID, page, size int64) ([]*ListingView, int64, error) {
	if eventID == 0 {
		return nil, 0, errors.BadRequest("LISTING_BAD_REQUEST", "活动 ID 不能为空")
	}
	list, total, err := uc.listingRepo.ListActiveByEvent(ctx, eventID, append(pageOptions(page, size),
		utils.ListOrderBy("marketplace_listings.created_at DESC"),
	)...)
	if err != nil {
		return nil, 0, err
	}
	views, err := uc.enrichListings(ctx, list)
	return views, total, err
}

func (uc *MarketplaceUseCase) ListMyListings(ctx context.Context, sellerID, page, size int64) ([]*ListingView, int64, error) {
	if sellerID == 0 {
		return nil, 0, errors.BadRequest("LISTING_BAD_REQUEST", "卖家 ID 不能为空")
	}
	list, total, err := uc.listingRepo.ListListings(ctx, append(pageOptions(page, size),
		utils.ListFilter("seller_id", uint(sellerID)),
		utils.ListOrderBy("created_at DESC"),
	)...)
	if err != nil {
		return nil, 0, err
	}
	views, err := uc.enrichListings(ctx, list)
	return views, total, err
}

func (uc *MarketplaceUseCase) ListMyPurchases(ctx context.Context, buyerID, page, size int64) ([]*ListingView, int64, error) {
	if buyerID == 0 {
		return nil, 0, errors.BadRequest("LISTING_BAD_REQUEST", "买家 ID 不能为空")
	}
	list, total, err := uc.listingRepo.ListListings(ctx, append(pageOptions(page, size),
		utils.ListFilter("buyer_id", uint(buyerID)),
		utils.ListOrderBy("created_at DESC"),
	)...)
	if err != nil {
		return nil, 0, err
	}
	views, err := uc.enrichListings(ctx, list)
	return views, total, err
}

// pageOptions 归一化分页参数并生成 Offset/Limit 选项。
func pageOptions(page, size int64) []utils.ListOption {
	if page <= 0 {
		page = 1
	}
	if size <= 0 {
		size = 10
	}
	return []utils.ListOption{
		utils.ListOffset(int((page - 1) * size)),
		utils.ListLimit(int(size)),
	}
}

// enrichListings 批量加载票、票种、活动，组装展示视图，避免 N+1 查询。
func (uc *MarketplaceUseCase) enrichListings(ctx context.Context, listings []*model.MarketplaceListing) ([]*ListingView, error) {
	views := make([]*ListingView, 0, len(listings))
	if len(listings) == 0 {
		return views, nil
	}

	// 收集票 ID 并批量查询
	ticketIDs := make([]int64, 0, len(listings))
	seen := make(map[uint]bool)
	for _, l := range listings {
		if !seen[l.TicketID] {
			ticketIDs = append(ticketIDs, int64(l.TicketID))
			seen[l.TicketID] = true
		}
	}
	tickets, err := uc.catalogRepo.GetTicketsByIDs(ctx, ticketIDs)
	if err != nil {
		return nil, err
	}
	ticketMap := make(map[uint]*model.Ticket, len(tickets))
	for _, t := range tickets {
		ticketMap[t.ID] = t
	}

	// 收集票种、活动 ID，批量查询名称
	ttIDs := make([]int64, 0)
	eventIDs := make([]int64, 0)
	ttSeen := make(map[uint]bool)
	eventSeen := make(map[uint]bool)
	for _, t := range ticketMap {
		if !ttSeen[t.TicketTypeID] {
			ttIDs = append(ttIDs, int64(t.TicketTypeID))
			ttSeen[t.TicketTypeID] = true
		}
		if !eventSeen[t.EventID] {
			eventIDs = append(eventIDs, int64(t.EventID))
			eventSeen[t.EventID] = true
		}
	}

	ttMap := make(map[uint]string)
	if len(ttIDs) > 0 {
		tts, err := uc.catalogRepo.GetTicketTypesByIDs(ctx, ttIDs)
		if err != nil {
			return nil, err
		}
		for _, tt := range tts {
			ttMap[tt.ID] = tt.Name
		}
	}

	eventMap := make(map[uint]string)
	if len(eventIDs) > 0 {
		events, err := uc.catalogRepo.GetEventsByIDs(ctx, eventIDs)
		if err != nil {
			return nil, err
		}
		for _, e := range events {
			eventMap[e.ID] = e.Title
		}
	}

	for _, l := range listings {
		view := &ListingView{MarketplaceListing: l}
		if ticket, ok := ticketMap[l.TicketID]; ok {
			view.EventID = ticket.EventID
			view.EventTitle = eventMap[ticket.EventID]
			view.TicketName = ttMap[ticket.TicketTypeID]
		}
		views = append(views, view)
	}
	return views, nil
}
