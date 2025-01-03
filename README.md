# my-redis

my-redis 是一个简单的 Redis 客户端和服务器实现，使用 Rust 语言编写。

## 功能特性

- 支持 Redis 的基本命令，如 `GET`、`SET`、`SUBSCRIBE`、`PUBLISH` 等。
- 基于 Tokio 实现异步 I/O，提供高性能的网络通信。
- 提供命令行工具 `my-redis-cli`，方便用户与 Redis 服务器进行交互。

## 项目结构

- `src/bin/client.rs`: 命令行客户端的入口点，用于解析命令行参数并执行相应的 Redis 命令。
- `src/bin/server.rs`: Redis 服务器的入口点，负责启动服务器并处理客户端连接。
- `src/cmd/`: 包含各个 Redis 命令的实现，如 `subscribe.rs`、`publish.rs`、`set.rs`、`get.rs` 等。
- `src/parse.rs`: 负责解析 Redis 协议帧。
- `src/connection.rs`: 处理客户端与服务器之间的网络连接和数据传输。
- `src/frame.rs`: 定义 Redis 协议帧的结构和解析逻辑。

## 依赖项

- `bytes`: 用于高效的字节处理。
- `tokio`: 提供异步 I/O 和并发编程的支持。
- `atoi`: 用于快速解析整数。
- `tracing`: 用于日志记录和调试。
- `tokio-stream`: 提供异步流处理的工具。
- `tracing-subscriber`: 用于配置和初始化日志记录。
- `clap`: 用于解析命令行参数。
- `async-stream`: 用于创建异步流。

## 安装

确保你已经安装了 Rust 编程语言和 Cargo 包管理器。然后，在项目根目录下运行以下命令来构建和运行项目：

```bash
cargo build
cargo run --bin my-redis-server
cargo run --bin my-redis-cli -- --addr 127.0.0.1 --port 6379
```

## 使用示例
## 启动Redis服务器
```bash
cargo run --bin my-redis-server
```

## 使用命令行客户端
```bash
cargo run --bin my-redis-cli -- --addr 127.0.0.1 --port 6379
```

## 执行GET命令
```bash
cargo run --bin my-redis-cli -- --addr 127.0.0.1 --port 6379 get key
```
## 执行SET命令
```bash
cargo run --bin my-redis-cli -- --addr 127.0.0.1 --port 6379 set key value
```

## 执行SUBSCRIBE命令
```bash
cargo run --bin my-redis-cli -- --addr 127.0.0.1 --port 6379 subscribe channel
```

## 执行PUBLISH命令
```bash
cargo run --bin my-redis-cli -- --addr 127.0.0.1 --port 6379 publish channel message
```

## 贡献
贡献者可以提交 pull request 来改进项目。

## 许可证
my-redis 使用 MIT 许可证。
