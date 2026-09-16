# User Service

用户服务，基于 [Kratos](https://go-kratos.dev/) 微服务框架：Protobuf 优先的 API
契约，gRPC / HTTP 双传输，Wire 依赖注入，PostgreSQL（GORM）持久化。

## 目录树

```text
user/
├── api/                          # 对外 API 契约（公共）
│   └── user/
│       └── v1/
│           ├── user.proto        # 用户服务 Protobuf 接口与消息定义（DTO 源头）
│           ├── user.pb.go        #   生成：protobuf 消息代码，勿手改
│           └── user_grpc.pb.go   #   生成：gRPC 服务桩代码，勿手改
├── cmd/                          # 应用入口
│   └── user/
│       ├── main.go               # 进程入口：加载配置、启动 server
│       ├── wire.go               # Wire 依赖注入声明
│       └── wire_gen.go           #   生成：Wire 装配代码，勿手改
├── configs/
│   └── config.yaml               # 运行时配置（HTTP/gRPC/PostgreSQL/Redis）
├── internal/
│   ├── biz/                      # 业务层：领域模型、用例、Repo 接口
│   │   ├── biz.go                #   biz ProviderSet
│   │   └── user.go               #   UserUseCase 与 UserRepo 接口（DO）
│   ├── dao/                      # 持久化对象（PO）
│   │   └── user.go               #   User GORM 表模型（密码 json:"-" 不外露）
│   ├── data/                     # 仓储实现：DO ↔ PO
│   │   ├── data.go               #   共享存储客户端（DB / Redis）初始化
│   │   └── user.go               #   UserRepo 的 GORM 实现
│   ├── server/                   # 传输层装配
│   │   ├── grpc.go               #   gRPC Server 构建与服务注册
│   │   └── server.go             #   HTTP/gRPC Server ProviderSet
│   └── service/                  # 服务层：DTO ↔ DO，请求校验
│       ├── README.md             #   service 层说明
│       ├── service.go            #   service ProviderSet
│       └── user.go               #   UserService 传输适配器
├── AGENTS.md                     # 分层契约与开发规范
├── CLAUDE.md                     # 同 AGENTS.md
├── Dockerfile                    # 容器镜像构建
├── LICENSE
├── Makefile                      # init / api / config / build / run / all
├── buf.gen.config.yaml           # buf 生成模板：配置 protobuf
├── buf.gen.yaml                  # buf 生成模板：API protobuf
├── buf.lock                      # buf 依赖锁定
├── buf.yaml                      # buf module 配置
├── go.mod
├── go.sum
├── openapi.yaml                  # 由 proto 生成的 OpenAPI 文档
└── README.md
```

> `.claude/`、`.DS_Store` 等本地工具/系统文件未列入。

## 分层与调用方向

```text
client ──► DTO ──► service ──► DO ──► biz ──► DO ──► data ──► PO(dao) ──► PostgreSQL
                                   ▲                 ▲
                                   │ 声明 Repo 接口   │ 实现接口
                                   └─────────────────┘
```

- `service`：只做 DTO ↔ DO 转换与入参校验，不接触存储。
- `biz`：拥有 DO、`UserUseCase` 与 `UserRepo` 接口，不含 proto/存储细节。
- `dao`：GORM 表模型（PO），描述 `User` 表结构。
- `data`：实现 `biz.UserRepo`，持有共享的 DB/Redis 客户端，负责 DO ↔ PO。
- `server`：构建 HTTP/gRPC Server、注册中间件与服务，无业务逻辑。
- `cmd`：唯一通过 Wire 组装所有层的位置。

## 常用命令

```bash
make init      # 安装 wire、buf 等生成工具
make api       # 由 proto 重新生成 api 代码与 OpenAPI
make config    # 生成配置相关 protobuf
make all       # api + config + wire + go mod tidy
make build     # 构建到 ./bin/
make run       # go run ./cmd/user -conf ./configs
go test ./...  # 运行测试
```

## 本地运行

```bash
go run ./cmd/user -conf ./configs
```

默认监听地址（见 `configs/config.yaml`）：

- HTTP：`0.0.0.0:8000`
- gRPC：`0.0.0.0:9000`

## Docker

```bash
docker build -t user-service .
docker run --rm -p 8000:8000 -p 9000:9000 \
  -v "$PWD/configs":/data/conf \
  user-service
```

## 注意事项

- `*.pb.go`、`*_grpc.pb.go`、`wire_gen.go` 均为生成文件，禁止手改；改源文件后执行
  `make all` 重新生成。
- `configs/config.yaml` 中不得提交真实数据库/Redis 凭证，请通过环境变量或部署时注入。
