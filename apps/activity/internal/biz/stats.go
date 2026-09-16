package biz

import "log/slog"

type StatsRepo interface{}

type StatsUseCase struct {
	repo   StatsRepo
	logger *slog.Logger
}

func NewStatsUseCase(repo StatsRepo, logger *slog.Logger) *StatsUseCase {
	return &StatsUseCase{
		repo:   repo,
		logger: logger,
	}
}
