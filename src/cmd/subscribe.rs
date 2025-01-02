use std::collections::HashMap;
use bytes::Bytes;
use tokio::select;
use tokio::sync::broadcast;
use tokio_stream::StreamExt;
use crate::cmd::{Command, Unknown};
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
        let mut subs = HashMap::new();

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
subscriptions:&mut HashMap<String,broadcast::Receiver<Bytes>>,
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
subscriptions:&mut HashMap<String,broadcast::Receiver<Bytes>>,
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

fn make_subscribe_frame(channel_name:String,num_subs:usize)->Frame{
    let mut response = Frame::array();
    response.push_bulk(Bytes::from_static(b"subscribe"));
    response.push_bulk(Bytes::from(channel_name));
    response.push_int(num_subs as u64);
    response
}

fn make_unsubscribe_frame(channel_name:String,num_subs:usize)->Frame{
    let mut response = Frame::array();
    response.push_bulk(Bytes::from_static(b"unsubscribe"));
    response.push_bulk(Bytes::from(channel_name));
    response.push_int(num_subs as u64);
    response
}

fn make_message_frame(channel_name:String,msg:Bytes)->Frame{
    let mut response = Frame::array();
    response.push_bulk(Bytes::from_static(b"message"));
    response.push_bulk(Bytes::from(channel_name));
    response.push_bulk(msg);
    response
}

impl Unsubscribe{
    pub(crate) fn new(channels:&[String])->Unsubscribe{
        Unsubscribe{
            channels: channels.to_vec(),
        }
    }

    pub(crate) fn parse_frames(parse:&mut Parse)->Result<Unsubscribe,ParseError>{
        use ParseError::EndOfStream;

        let mut channels = vec![];

        loop{
            match parse.next_string(){
                Ok(s)=>channels.push(s),
                Err(EndOfStream)=>break,
                Err(e)=>return Err(e.into()),
            }
        }
        Ok(Unsubscribe{channels})
    }

    pub(crate) fn into_frame(self)->Frame{
        let mut frame= Frame::array();
        frame.push_bulk(Bytes::from("unsubscribe".as_bytes()));
        for channel in self.channels{
            frame.push_bulk(Bytes::from(channel.into_bytes()));
        }
        frame
    }
}