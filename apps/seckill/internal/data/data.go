package data

import (
	"log/slog"
	"sale/pkg/conf"
	"sale/pkg/database"
	"sale/pkg/rdb"

	"github.com/go-kratos/kratos/v3/log"
	"github.com/google/wire"
	"github.com/redis/go-redis/v9"
	"gorm.io/gorm"
)

// ProviderSet is data providers.
var ProviderSet = wire.NewSet(NewData, NewSkillRepo, NewRdbRepo)

// Data holds the long-lived storage clients shared by repos.
type Data struct {
	db     *gorm.DB
	rdb    *redis.Client
	logger *slog.Logger
}

// NewData opens the database client and returns it with a cleanup function.
func NewData(c *conf.Data, logger *slog.Logger) (*Data, func(), error) {
	db := database.NewPgDataBase(c)
	rClient := rdb.NewRdbClient(c.GetRedis())
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
	return &Data{db: db, rdb: rClient}, cleanup, nil
}
