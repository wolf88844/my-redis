use bytes::Bytes;
use tokio::select;
use tokio::sync::broadcast;
use crate::cmd::Command;
use crate::cmd::Command::Unknown;
use crate::connection::Connection;
use crate::db::Db;
use crate::frame::Frame;
use crate::parse::{Parse, ParseError};
use crate::shutdown::Shutdown;
#[derive(Debug)]
pub struct Subscribe {
    channels: Vec<String>,
}

#[derive(Debug)]
pub struct Unsubscribe {
    channels: Vec<String>,
}

impl Subscribe {
    pub(crate) fn new(channels:&[String])->Subscribe{
        Subscribe {
            channels: channels.to_vec(),
        }
    }

    pub(crate) fn parse_frames(parse:&mut Parse)->crate::Result<Subscribe>{
        use ParseError::EndOfStream;

        let mut channels = vec![parse.next_string()?];

        loop{
            match parse.next_string(){
                Ok(s)=>channels.push(s),
                Err(EndOfStream)=>break,
                Err(e)=>return Err(e.into()),
            }
        }
        Ok(Subscribe{channels })
    }

    pub(crate) async fn apply(mut self,db:&Db,dst:&mut Connection,shutdow:&mut Shutdown)->crate::Result<()>{
        let mut subs = StreamExt::new();

        loop{
            for channel_name in self.channels.drain(..){
                subscribe_to_channel(channel_name,&mut subs,db,dst).await?;
            }
            select!{
                Some((channel_name,msg))=subs.next()=>{
                    use tokio::sync::broadcast::RecvError;
                    let msg = match msg{
                        Ok(msg)=>msg,
                        Err(RecvError::Lagged(_))=>continue,
                        Err(RecvError::Closed)=>unreachable!(),
                    };
                    dst.write_frame(&make_message_frame(channel_name,msg)).await?;
                }
                res = dst.read_frame()=>{
                    let frame = match res?{
                        Some(frame)=>frame,
                        None=>return Ok(()),
                    };
                    handle_command(
                        frame,
                        &mut self.channels,
                        &mut subs,
                        dst,
                    ).await?;
                }
                _=shutdow.recv()=>return Ok(()),
            }
        }
    }

    pub(crate) fn into_frame(self)->Frame{
        let mut frame = Frame::array();
        frame.push_bulk(Bytes::from("subscribe".as_bytes()));
        for channel in self.channels{
            frame.push_bulk(Bytes::from(channel.into_bytes()));
        }
        frame
    }
}

async fn subscribe_to_channel(channel_name:String,
subscriptions:&mut StreamMap<String,broadcast::Receiver<Bytes>>,
db:&Db,
dst:&mut Connection,
)->crate::Result<()>{
    let rx = db.subscribe(channel_name.clone());
    subscriptions.insert(channel_name.clone(),rx);
    let response = make_subscribe_frame(channel_name,subscriptions.len());
    dst.write_frame(&response).await?;
    Ok(())
}

async fn handle_command(frame:Frame,subscribe_to:&mut Vec<String>,
subscriptions:&mut StreamMap<String,broadcast::Receiver<Bytes>>,
dst:&mut Connection)->crate::Result<()>{
    match Command::from_frame(frame)? {
        Command::Subscribe(subscribe)=>{
            subscribe_to.extend(subscribe.channels.into_iter());
        }
        Command::Unsubscribe(mut unsubscribe)=>{
            if unsubscribe.channels.is_empty(){
                unsubscribe.channels = subscriptions
                    .keys()
                    .map(|channel_name|channel_name.to_string())
                    .collect();
            }
            for channel_name in unsubscribe.channels{
                subscriptions.remove(&channel_name);
                let response = make_unsubscribe_frame(channel_name,subscriptions.len());
                dst.write_frame(&response).await?;
            }
        }
        command=>{
            let cmd = Unknown::new(command.get_name());
            cmd.apply(dst).await?;
        }
    }
    Ok(())
}

