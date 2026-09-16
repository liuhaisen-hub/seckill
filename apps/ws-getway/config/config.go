// Package config 负责加载 getway 服务的配置文件，
// 并扫描进 pkg/conf 中由 proto 生成的 Bootstrap 结构体。
package config

import (
	"flag"

	"github.com/go-kratos/kratos/v3/config"
	"github.com/go-kratos/kratos/v3/config/env"
	"github.com/go-kratos/kratos/v3/config/file"

	"sale/pkg/conf"
)

// flagconf 是配置文件路径，可通过 -conf 覆盖。
var flagconf string

// C 是加载完成后的全局配置，须在 Init 成功后使用。
var C *conf.Bootstrap

func init() {
	flag.StringVar(&flagconf, "conf", "config/config.yaml", "config path, eg: -conf config/config.yaml")
}

// Init 加载配置文件并扫描到全局 C，失败时直接 panic。
func Init() {
	c := config.New(
		config.WithSource(
			file.NewSource(flagconf),
			env.NewSource("KRATOS"),
		),
	)
	defer c.Close()

	if err := c.Load(); err != nil {
		panic(err)
	}

	var bc conf.Bootstrap
	if err := c.Scan(&bc); err != nil {
		panic(err)
	}
	C = &bc
}
