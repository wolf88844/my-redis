mod cmd;
mod connection;
mod db;
mod frame;
mod parse;
pub mod server;
mod shutdown;
pub mod client;

pub type Error = Box<dyn std::error::Error + Send + Sync>;
pub type Result<T> = std::result::Result<T, Error>;

pub const DEFAULT_PORT: &str = "6379";
