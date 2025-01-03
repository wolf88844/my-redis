mod get;
mod publish;
mod set;
mod subscribe;
mod unknown;

use crate::connection::Connection;
use crate::db::Db;
use crate::frame::Frame;
use crate::parse::Parse;
use crate::shutdown::Shutdown;
pub use get::Get;
pub use publish::Publish;
pub use set::Set;
pub use subscribe::Subscribe;
pub use subscribe::Unsubscribe;
pub use unknown::Unknown;

#[derive(Debug)]
pub enum Command {
    Get(Get),
    Publish(Publish),
    Set(Set),
    Subscribe(Subscribe),
    Unsubscribe(Unsubscribe),
    Unknown(Unknown),
}

impl Command {
    pub fn from_frame(frame: Frame) -> crate::Result<Command> {
        let mut parse = Parse::new(frame)?;
        let command_name = parse.next_string()?.to_lowercase();
        let command = match &command_name[..] {
            "get" => Command::Get(Get::parse_frames(&mut parse)?),
            "publish" => Command::Publish(Publish::parse_frames(&mut parse)?),
            "set" => Command::Set(Set::parse_frames(&mut parse)?),
            "subscribe" => Command::Subscribe(Subscribe::parse_frames(&mut parse)?),
            "unsubscribe" => Command::Unsubscribe(Unsubscribe::parse_frames(&mut parse)?),
            _ => return Ok(Command::Unknown(Unknown::new(command_name))),
        };
        parse.finish()?;
        Ok(command)
    }

    pub(crate) async fn apply(
        self,
        db: &Db,
        dst: &mut Connection,
        shutdown: &mut Shutdown,
    ) -> crate::Result<()> {
        use Command::*;
        match self {
            Get(cmd) => cmd.apply(db, dst).await,
            Publish(cmd) => cmd.apply(db, dst).await,
            Set(cmd) => cmd.apply(db, dst).await,
            Subscribe(cmd) => cmd.apply(db, dst, shutdown).await,
            Unsubscribe(_) => Err("unsubscribe si unsupproted in this context".into()),
            Unknown(cmd) => cmd.apply(dst).await,
        }
    }

    pub(crate) fn get_name(&self) -> &str {
        match self {
            Command::Get(_) => "get",
            Command::Publish(_) => "pub",
            Command::Set(_) => "set",
            Command::Subscribe(_) => "subscribe",
            Command::Unsubscribe(_) => "unsubscribe",
            Command::Unknown(cmd) => cmd.get_name(),
        }
    }
}
