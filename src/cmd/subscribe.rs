use crate::cmd::{Command, Unknown};
use crate::connection::Connection;
use crate::db::Db;
use crate::frame::Frame;
use crate::parse::{Parse, ParseError};
use crate::shutdown::Shutdown;
use bytes::Bytes;
use tokio::select;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::{StreamExt, StreamMap};
#[derive(Debug)]
pub struct Subscribe {
    channels: Vec<String>,
}

#[derive(Debug)]
pub struct Unsubscribe {
    channels: Vec<String>,
}

impl Subscribe {
    pub(crate) fn new(channels: &[String]) -> Subscribe {
        Subscribe {
            channels: channels.to_vec(),
        }
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Subscribe> {
        use ParseError::EndOfStream;

        let mut channels = vec![parse.next_string()?];

        loop {
            match parse.next_string() {
                Ok(s) => channels.push(s),
                Err(EndOfStream) => break,
                Err(e) => return Err(e.into()),
            }
        }
        Ok(Subscribe { channels })
    }

    /// 应用订阅命令到数据库和连接
    ///
    /// 该函数处理订阅命令，将订阅的频道添加到流映射中，并在接收到消息时将其发送回客户端。
    /// 它还处理来自客户端的命令，并在接收到关闭信号时优雅地关闭连接。
    ///
    /// # 参数
    ///
    /// * `self` - 订阅命令的可变引用
    /// * `db` - 数据库的不可变引用
    /// * `dst` - 客户端连接的可变引用
    /// * `shutdow` - 关闭信号的可变引用
    ///
    /// # 返回值
    ///
    /// 返回一个 `crate::Result<()>`，表示操作的成功或失败。
    pub(crate) async fn apply(
        mut self,
        db: &Db,
        dst: &mut Connection,
        shutdow: &mut Shutdown,
    ) -> crate::Result<()> {
        // 创建一个新的流映射来存储订阅的频道和它们的接收器
        let mut subs = StreamMap::new();
        loop {
            // 遍历所有要订阅的频道
            for channel_name in self.channels.drain(..) {
                // 为每个频道订阅并将其添加到流映射中
                subscribe_to_channel(channel_name, &mut subs, db, dst).await?;
            }
            // 使用 `select!` 宏来同时等待多个异步操作
            select! {
                // 当从订阅的频道接收到消息时
                Some((channel_name,msg))=subs.next()=>{
                    // 处理接收到的消息
                    let msg = match msg{
                        Ok(msg) => msg,
                        Err(_) => unreachable!(),
                    };
                    // 将消息发送回客户端
                    dst.write_frame(&make_message_frame(channel_name,msg)).await?;
                }
                // 当从客户端接收到命令时
                res = dst.read_frame()=>{
                    // 处理接收到的命令
                    let frame = match res?{
                        Some(frame)=>frame,
                        None=>return Ok(()),
                    };
                    handle_command(frame,&mut self.channels,&mut subs,dst).await?;
                }
                // 当接收到关闭信号时
                _=shutdow.recv()=>return Ok(()),
            }
        }
    }

    pub(crate) fn into_frame(self) -> Frame {
        let mut frame = Frame::array();
        frame.push_bulk(Bytes::from("subscribe".as_bytes()));
        for channel in self.channels {
            frame.push_bulk(Bytes::from(channel.into_bytes()));
        }
        frame
    }
}

async fn subscribe_to_channel(
    channel_name: String,
    subscriptions: &mut StreamMap<String, BroadcastStream<Bytes>>,
    db: &Db,
    dst: &mut Connection,
) -> crate::Result<()> {
    let rx = db.subscribe(channel_name.clone());
    subscriptions.insert(channel_name.clone(), BroadcastStream::new(rx));
    let response = make_subscribe_frame(channel_name, subscriptions.len());
    dst.write_frame(&response).await?;
    Ok(())
}

async fn handle_command(
    frame: Frame,
    subscribe_to: &mut Vec<String>,
    subscriptions: &mut StreamMap<String, BroadcastStream<Bytes>>,
    dst: &mut Connection,
) -> crate::Result<()> {
    match Command::from_frame(frame)? {
        Command::Subscribe(subscribe) => {
            subscribe_to.extend(subscribe.channels.into_iter());
        }
        Command::Unsubscribe(mut unsubscribe) => {
            if unsubscribe.channels.is_empty() {
                unsubscribe.channels = subscriptions
                    .keys()
                    .map(|channel_name| channel_name.to_string())
                    .collect();
            }
            for channel_name in unsubscribe.channels {
                subscriptions.remove(&channel_name);
                let response = make_unsubscribe_frame(channel_name, subscriptions.len());
                dst.write_frame(&response).await?;
            }
        }
        command => {
            let cmd = Unknown::new(command.get_name());
            cmd.apply(dst).await?;
        }
    }
    Ok(())
}

fn make_subscribe_frame(channel_name: String, num_subs: usize) -> Frame {
    let mut response = Frame::array();
    response.push_bulk(Bytes::from_static(b"subscribe"));
    response.push_bulk(Bytes::from(channel_name));
    response.push_int(num_subs as u64);
    response
}

fn make_unsubscribe_frame(channel_name: String, num_subs: usize) -> Frame {
    let mut response = Frame::array();
    response.push_bulk(Bytes::from_static(b"unsubscribe"));
    response.push_bulk(Bytes::from(channel_name));
    response.push_int(num_subs as u64);
    response
}

fn make_message_frame(channel_name: String, msg: Bytes) -> Frame {
    let mut response = Frame::array();
    response.push_bulk(Bytes::from_static(b"message"));
    response.push_bulk(Bytes::from(channel_name));
    response.push_bulk(msg);
    response
}

impl Unsubscribe {
    pub(crate) fn new(channels: &[String]) -> Unsubscribe {
        Unsubscribe {
            channels: channels.to_vec(),
        }
    }

    pub(crate) fn parse_frames(parse: &mut Parse) -> Result<Unsubscribe, ParseError> {
        use ParseError::EndOfStream;

        let mut channels = vec![];

        loop {
            match parse.next_string() {
                Ok(s) => channels.push(s),
                Err(EndOfStream) => break,
                Err(e) => return Err(e.into()),
            }
        }
        Ok(Unsubscribe { channels })
    }

    pub(crate) fn into_frame(self) -> Frame {
        let mut frame = Frame::array();
        frame.push_bulk(Bytes::from("unsubscribe".as_bytes()));
        for channel in self.channels {
            frame.push_bulk(Bytes::from(channel.into_bytes()));
        }
        frame
    }
}
