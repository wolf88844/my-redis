use std::{num::ParseIntError, str, time::Duration};

use bytes::Bytes;
use clap::{Parser, command};
use my_redis::client;

#[derive(Parser, Debug)]
#[command(name = "my-redis-cli", version = env!("CARGO_PKG_VERSION"), author = env!("CARGO_PKG_AUTHORS"), about = "Issue Redis commands")]
/// `Cli` 结构体用于解析命令行参数。
/// 它包含一个子命令和 Redis 服务器的主机和端口信息。
struct Cli {
    /// 子命令，用于指定要执行的 Redis 操作。
    #[structopt(subcommand)]
    command: Command,

    /// Redis 服务器的主机地址。
    #[arg(short, long, help = "Redis host")]
    addr: String,

    /// Redis 服务器的端口号。
    #[arg(short, long, help = "Redis port")]
    port: String,
}

#[derive(Debug, Parser)]
enum Command {
    Get {
        key: String,
    },
    Set {
        key: String,
        #[arg(short,long,value_parser=bytes_from_str)]
        value: Bytes,
        #[arg(short,long,value_parser=duration_from_str)]
        expires: Option<Duration>,
    },
}

fn duration_from_str(s: &str) -> Result<Duration, ParseIntError> {
    let ms = s.parse::<u64>()?;
    Ok(Duration::from_millis(ms))
}

fn bytes_from_str(s: &str) -> Result<Bytes, ParseIntError> {
    Ok(Bytes::from(s.to_string()))
}

#[tokio::main]
async fn main() -> my_redis::Result<()> {
    tracing_subscriber::fmt::try_init()?;

    let cli = Cli::parse();

    let addr = format!("{}:{}", cli.addr, cli.port);

    let mut client = client::connect(&addr).await?;

    match cli.command {
        Command::Get { key } => {
            if let Some(value) = client.get(&key).await? {
                if let Ok(string) = str::from_utf8(&value) {
                    println!("\"{}\"", string);
                } else {
                    println!("{:?}", value);
                }
            } else {
                println!("(nil)")
            }
        }
        Command::Set {
            key,
            value,
            expires: None,
        } => {
            client.set(&key, value).await?;
            println!("OK");
        }
        Command::Set {
            key,
            value,
            expires: Some(expires),
        } => {
            client.set_expires(&key, value, expires).await?;
            println!("OK");
        }
    }

    Ok(())
}
