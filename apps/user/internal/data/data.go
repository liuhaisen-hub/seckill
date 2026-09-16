package data

import (
	"log/slog"
	"sale/pkg/conf"
	"sale/pkg/database"

	"github.com/go-kratos/kratos/v3/log"
	"github.com/google/wire"
	"gorm.io/gorm"
)

// ProviderSet is data providers.
var ProviderSet = wire.NewSet(NewData, NewUserRepo)

// Data holds the long-lived storage clients shared by repos.
type Data struct {
	db     *gorm.DB
	logger *slog.Logger
}

// NewData opens the database client and returns it with a cleanup function.
func NewData(c *conf.Data, logger *slog.Logger) (*Data, func(), error) {
	db := database.NewPgDataBase(c)
	cleanup := func() {
		log.Info("closing the data resources")
		pg, err := db.DB()
		if err != nil {
			return
		}
		if err := pg.Close(); err != nil {
			logger.Error("failed closing the database", "err", err)
		}
	}
	return &Data{db: db}, cleanup, nil
}
