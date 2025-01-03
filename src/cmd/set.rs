use crate::connection::Connection;
use crate::db::Db;
use crate::frame::Frame;
use crate::parse::{Parse, ParseError};
use bytes::Bytes;
use std::time::Duration;
use tracing::debug;

#[derive(Debug)]
pub struct Set {
    key: String,
    value: Bytes,
    expire: Option<Duration>,
}

impl Set {
    pub fn new(key: impl ToString, value: Bytes, expire: Option<Duration>) -> Self {
        Self {
            key: key.to_string(),
            value,
            expire,
        }
    }

    #[allow(dead_code)]
    pub fn key(&self) -> &str {
        &self.key
    }
    #[allow(dead_code)]
    pub fn value(&self) -> &Bytes {
        &self.value
    }
    #[allow(dead_code)]
    pub fn expire(&self) -> Option<Duration> {
        self.expire
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Set> {
        use ParseError::EndOfStream;

        let key = parse.next_string()?;
        let value = parse.next_bytes()?;
        let mut expire = None;

        match parse.next_string() {
            Ok(s) if s == "EX" => {
                let secs = parse.next_int()?;
                expire = Some(Duration::from_secs(secs));
            }
            Ok(s) if s == "PX" => {
                let ms = parse.next_int()?;
                expire = Some(Duration::from_millis(ms));
            }
            Ok(_) => return Err("currently `set` only supports the expiration option".into()),
            Err(EndOfStream) => {}
            Err(e) => return Err(e.into()),
        }
        Ok(Set { key, value, expire })
    }

    pub(crate) async fn apply(self, db: &Db, dst: &mut Connection) -> crate::Result<()> {
        db.set(self.key, self.value, self.expire);
        let response = Frame::Simple("OK".to_string());
        debug!(?response);
        dst.write_frame(&response).await?;
        Ok(())
    }

    pub(crate) fn into_frame(self) -> Frame {
        let mut frame = Frame::array();
        frame.push_bulk(Bytes::from("set".as_bytes()));
        frame.push_bulk(Bytes::from(self.key.into_bytes()));
        frame.push_bulk(self.value);
        frame
    }
}
