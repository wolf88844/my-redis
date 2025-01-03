mod cmd;
mod connection;
mod db;
mod frame;
mod parse;
mod server;
mod shutdown;
pub type Error = Box<dyn std::error::Error + Send + Sync>;
pub type Result<T> = std::result::Result<T, Error>;

pub const DEFAULT_PORT: &str = "6379";
