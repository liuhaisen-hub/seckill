package data

import (
	"log/slog"

	"activity/internal/biz"
)

type statsRepo struct {
	data   *Data
	logger *slog.Logger
}

func NewStatsRepo(data *Data, logger *slog.Logger) biz.StatsRepo {
	return &statsRepo{
		data:   data,
		logger: logger,
	}
}
