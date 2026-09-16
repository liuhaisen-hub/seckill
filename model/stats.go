package model

// 统计相关

type DashboardStats struct {
	TotalEvents     int64   `json:"total_events"`
	ActiveEvents    int64   `json:"active_events"`
	TotalTickets    int64   `json:"total_tickets"`
	SoldTickets     int64   `json:"sold_tickets"`
	ReservedTickets int64   `json:"reserved_tickets"`
	TotalRevenue    float64 `json:"total_revenue"`
	TodaySales      int64   `json:"today_sales"`
	TodayRevenue    float64 `json:"today_revenue"`
}

type SalesTrend struct {
	Date    string  `json:"date"`
	Count   int64   `json:"count"`
	Revenue float64 `json:"revenue"`
}

type TicketTypeStats struct {
	TicketTypeID   uint    `json:"ticket_type_id"`
	TicketTypeName string  `json:"ticket_type_name"`
	EventTitle     string  `json:"event_title"`
	SoldCount      int64   `json:"sold_count"`
	Revenue        float64 `json:"revenue"`
}

type ConversionFunnel struct {
	PageViews int64 `json:"page_views"`
	AddToCart int64 `json:"add_to_cart"`
	Reserved  int64 `json:"reserved"`
	Paid      int64 `json:"paid"`
	Used      int64 `json:"used"`
}
