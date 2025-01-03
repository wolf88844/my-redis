use std::{io::{Error, ErrorKind}, time::Duration};

use async_stream::try_stream;
use bytes::Bytes;
use tokio::net::{TcpStream, ToSocketAddrs};
use tokio_stream::Stream;
use tracing::debug;

use crate::{cmd::{Get, Publish, Set, Subscribe, Unsubscribe}, connection::Connection, frame::Frame};


pub struct Client{
    connection:Connection,
}

pub struct Subscriber{
    client:Client,
    subscribed_channels:Vec<String>,
}

#[derive(Debug,Clone)]
pub struct Message{
    pub channel:String,
    pub content:Bytes,
}


pub async fn connect<T: ToSocketAddrs>(addr:T)->crate::Result<Client>{
    let socket = TcpStream::connect(addr).await?;
    let connection = Connection::new(socket);
    Ok(Client{connection})
}


impl Client{
    pub async fn get(&mut self,key:&str)->crate::Result<Option<Bytes>>{
        let frame = Get::new(key).into_frame();
        debug!(request=?frame);

        self.connection.write_frame(&frame).await?;

        match self.read_response().await?{
            Frame::Simple(value)=>Ok(Some(value.into())),
            Frame::Bulk(value)=>Ok(Some(value)),
            Frame::Null=>Ok(None),
            frame=>Err(frame.to_error()),
        }
    }

    pub async fn set(&mut self,key:&str,value:Bytes)->crate::Result<()>{
        self.set_cmd(Set::new(key, value, None)).await
    }

    pub async fn set_expires(&mut self,key:&str,value:Bytes,expires:Duration)->crate::Result<()>{
        self.set_cmd(Set::new(key, value, Some(expires))).await
    }

    async fn set_cmd(&mut self,set:Set)->crate::Result<()>{
        let frame = set.into_frame();

        debug!(request=?frame);

        self.connection.write_frame(&frame).await?;

        match self.read_response().await?{
            Frame::Simple(response)=>{
                if response=="OK"{
                    Ok(())
                }else{
                    Err("unexpected response".into())
                }
            }
            Frame::Error(err)=>Err(err.into()),
            frame=>Err(frame.to_error()),
        }
    }


    pub async fn publish(&mut self,channel:&str,message:Bytes)->crate::Result<u64>{
        let frame = Publish::new(channel, message).info_frame();
        debug!(request=?frame);

        self.connection.write_frame(&frame).await?;

        match self.read_response().await?{
            Frame::Integer(response)=>Ok(response),
            frame=>Err(frame.to_error()),
        }
    }

    pub async fn subscribe(mut self,channels:Vec<String>)->crate::Result<Subscriber>{
        self.subscribe_cmd(&channels).await?;
        Ok(Subscriber{client:self,subscribed_channels:channels})
    }

    async  fn subscribe_cmd(&mut self,channels:&[String]) ->crate::Result<()>{
        let frame = Subscribe::new(channels).into_frame();
        debug!(request=?frame);

        self.connection.write_frame(&frame).await?;

        for channel in channels{
            let response = self.read_response().await?;
            match response{
                Frame::Array(ref frame)=>match frame.as_slice(){
                    [subscribe,schannel,..]
                    if *subscribe == "subscribe" && *schannel ==channel=>{}
                    _=>return Err(response.to_error()),
                },
                frame=>return Err(frame.to_error()),
            };
        }
        Ok(())
    }

    async fn read_response(&mut self)->crate::Result<Frame>{
        let response = self.connection.read_frame().await?;
        debug!(?response);
        match response{
            Some(Frame::Error(msg))=>Err(msg.into()),
            Some(frame)=>Ok(frame),
            None=>{
                let err = Error::new(ErrorKind::ConnectionReset,"connection reset by server");
                Err(err.into())
            }
        }
    }

    
}

impl Subscriber{
    pub fn get_subscribed(&self)->&[String]{
        &self.subscribed_channels
    }

    pub async fn next_message(&mut self)->crate::Result<Option<Message>>{
        match self.client.connection.read_frame().await?{
            Some(mframe)=>{
                debug!(?mframe);
                match mframe{
                    Frame::Array(ref frame)=>match frame.as_slice(){
                        [message,channel,content] if *message =="message"=> Ok(Some(Message{
                            channel:channel.to_string(),
                            content:Bytes::from(content.to_string()),
                        })),
                        _=>Err(mframe.to_error()),
                    },
                    frame=>Err(frame.to_error()),
                }
            }
            None=>Ok(None),
        }
    }

    pub fn into_stream(mut self)-> impl Stream<Item=crate::Result<Message>>{
        try_stream!{
            while let Some(message) = self.next_message().await?{
                yield message;
            }
        }
    }

    pub async fn subscribe(&mut self,channels:&[String])->crate::Result<()>{
        self.client.subscribe_cmd(channels).await?;
        self.subscribed_channels.extend(channels.iter().map(Clone::clone));
        Ok(())
    }

    pub async fn unsubscribe(&mut self,channels:&[String])->crate::Result<()>{
        let frame = Unsubscribe::new(&channels).into_frame();

        debug!(request=?frame);

        self.client.connection.write_frame(&frame).await?;

        let num = if channels.is_empty(){
            self.subscribed_channels.len()
        }else{
            channels.len()
        };

        for _ in 0..num{
            let response = self.client.read_response().await?;

            match response{
                Frame::Array(ref frame)=>match frame.as_slice(){
                    [unsubscribe,channel,..] if *unsubscribe=="unsubscribe"=>{
                        let len = self.subscribed_channels.len();
                        if len==0{
                            return Err(response.to_error());
                        }

                        self.subscribed_channels.retain(|c| *channel!=&c[..]);

                        if self.subscribed_channels.len()!=len-1{
                            return Err(response.to_error());
                        }
                    }
                    _=>return Err(response.to_error()),
                }
                frame=>return Err(frame.to_error()),
            }
        }

        Ok(())
    }
}