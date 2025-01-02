use log::debug;
use crate::connection::Connection;
use crate::db::Db;
use crate::frame::Frame;
use crate::parse::Parse;

#[derive(Debug)]
pub struct Get {
    key: String,
}

impl Get {
    pub fn new(key: impl ToString) -> Get {
        Get {
            key: key.to_string(),
        }
    }

    pub fn key(&self) -> &str {
        &self.key
    }

    pub(crate) fn parse_frames(parse:&mut Parse)->crate::Result<Get>{
        let key = parse.next_string()?;
        Ok(Get { key })
    }

    pub(crate) async fn apply(self,db:&Db,dst:&mut Connection)->crate::Result<()>{
        let response = if let Some(value)=db.get(&self.key){
            Frame::Bulk(value)
        }else{
            Frame::Null
        };
        debug!(?response);

        dst.write_frame(&response).await?;
        Ok(())
    }
}
