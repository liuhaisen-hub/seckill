package database

import (
	"fmt"
	"sale/pkg/conf"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func NewPgDataBase(c *conf.Data) *gorm.DB {
	dc := c.GetDatabase()
	dsn := fmt.Sprintf("host=%s user=%s password=%s dbname=%s port=%d sslmode=disable",
		dc.GetHost(), dc.GetUserName(), dc.GetPassword(), dc.GetDatabase(), dc.GetPort())
	db, err := gorm.Open(postgres.New(postgres.Config{
		DSN:                  dsn,
		PreferSimpleProtocol: true, // 禁用隐式prepare statement
	}), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Info),
		// PrepareStmt: 缓存预编译语句。
		// 收益：省去每次执行的 SQL 解析和计划生成，高频查询可提速 20~30%。
		// 代价：每个连接维护一份语句缓存，占用少量内存。
		// ⚠️ 注意：如果前面挂了 PgBouncer 且 POOL_MODE=transaction，
		//    预编译语句会失效甚至报错，此时必须关掉这个选项。
		PrepareStmt: true,

		// SkipDefaultTransaction: 关闭 GORM 的隐式事务。
		// GORM 默认会把每个 Create/Update/Delete 包进一个事务（BEGIN...COMMIT），
		// 这对单条写入是纯粹的性能浪费（多两次网络往返）。
		// 关掉后单条写入性能提升约 30%。
		// 需要事务的地方我们会用 db.Transaction(...) 显式声明——显式优于隐式。
		SkipDefaultTransaction: true,
	})
	if err != nil {
		panic(err)
	}
	// // 拿到底层的 *sql.DB 来配置连接池（GORM 只是它的封装层）
	// sqlDB, err := db.DB()
	// // ===== 连接池四大参数 =====

	// // ① MaxOpenConns：最大连接数（使用中 + 空闲）
	// //    超过后新请求会阻塞等待，直到有连接释放或 context 超时。
	// sqlDB.SetMaxOpenConns(maxOpenConns)

	// // ② MaxIdleConns：最大空闲连接数。
	// //    设太小 → 连接频繁创建销毁，每次 TCP 握手 + TLS 握手 + PG 认证约 5~20ms，很贵。
	// //    设太大 → 空闲连接占用数据库的 max_connections 配额。
	// //    经验值：MaxIdleConns ≈ MaxOpenConns / 2。
	// sqlDB.SetMaxIdleConns(maxIdleConns)

	// // ③ ConnMaxLifetime：连接最大存活时间。
	// //    为什么必须设？因为链路上的 LB、防火墙、PgBouncer 可能在几分钟后
	// //    静默关闭 TCP 连接，而应用侧毫不知情，复用时就报 "connection reset by peer"。
	// //    主动定期重建连接就能规避。经验值 300s，且应小于中间件的空闲超时。
	// sqlDB.SetConnMaxLifetime(time.Duration(300) * time.Second)

	// // ④ ConnMaxIdleTime：连接最大空闲时间。
	// //    流量低谷时把多余连接还给数据库，避免长期占坑。
	// if connMaxIdleTime > 0 {
	// 	sqlDB.SetConnMaxIdleTime(time.Duration(connMaxIdleTime) * time.Second)
	// }
	return db

}
