use crate::frame;
use crate::frame::Frame;
use bytes::{Buf, BytesMut};
use std::io;
use std::io::Cursor;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufWriter};
use tokio::net::TcpStream;

#[derive(Debug)]
pub struct Connection {
    stream: BufWriter<TcpStream>,
    buffer: BytesMut,
}

impl Connection {
    pub fn new(socket: TcpStream) -> Connection {
        Connection {
            stream: BufWriter::new(socket),
            buffer: BytesMut::with_capacity(4096),
        }
    }

    pub async fn read_frame(&mut self) -> crate::Result<Option<Frame>> {
        loop {
            if let Some(frame) = self.parse_frame()? {
                return Ok(Some(frame));
            }
            if 0 == self.stream.read_buf(&mut self.buffer).await? {
                if self.buffer.is_empty() {
                    return Ok(None);
                } else {
                    return Err("connection reset by peer".into());
                }
            }
        }
    }

    /// 从 `Connection` 结构体的缓冲区中解析出一个 `Frame` 结构体
    /// 如果解析成功，则返回 `Ok(Some(Frame))`；如果解析失败，则返回相应的错误
    fn parse_frame(&mut self) -> crate::Result<Option<Frame>> {
        use frame::Error::Incomplete;

        // 创建一个新的 Cursor 对象，用于从缓冲区中读取数据
        let mut buf = Cursor::new(&self.buffer[..]);

        // 调用 Frame::check 方法检查缓冲区中的数据是否符合协议格式
        match Frame::check(&mut buf) {
            // 如果检查通过，则解析出一个 Frame 对象
            Ok(_) => {
                // 获取当前 Cursor 对象的位置，即已经读取的数据长度
                let len = buf.position() as usize;
                // 将 Cursor 对象的位置重置为 0，以便从头开始解析
                buf.set_position(0);
                // 调用 Frame::parse 方法解析出一个 Frame 对象
                let frame = Frame::parse(&mut buf)?;
                // 将缓冲区中的数据向前移动已经读取的数据长度
                self.buffer.advance(len);
                // 返回解析出的 Frame 对象
                Ok(Some(frame))
            }
            // 如果检查未通过，且错误类型为 Incomplete，则表示数据不完整，返回 None
            Err(Incomplete) => Ok(None),
            // 如果检查未通过，且错误类型为其他，则将错误转换为 crate::Result 类型并返回
            Err(e) => Err(e.into()),
        }
    }
    /// 将一个 `Frame` 结构体写入到 `Connection` 结构体的缓冲区中
    /// 如果写入成功，则返回 `Ok(())`；如果写入失败，则返回相应的错误
    pub async fn write_frame(&mut self, frame: &Frame) -> io::Result<()> {
        // 根据 Frame 结构体的不同类型，进行不同的处理
        match frame {
            // 如果是数组类型，则先写入一个 '*' 字符，然后写入数组的长度，最后遍历数组中的每个元素，递归调用 write_frame 函数写入每个元素
            Frame::Array(val) => {
                self.stream.write_u8(b'*').await?;
                self.write_decimal(val.len() as u64).await?;
                for entry in &**val {
                    Box::pin(self.write_frame(entry)).await?;
                }
            }
            // 如果是其他类型，则直接调用 write_value 函数写入值
            _ => self.write_value(frame).await?,
        }
        // 刷新缓冲区，确保数据被实际写入到连接中
        self.stream.flush().await?;
        // 返回成功
        Ok(())
    }

    /// 将一个 `Frame` 结构体的值写入到 `Connection` 结构体的缓冲区中
    /// 如果写入成功，则返回 `Ok(())`；如果写入失败，则返回相应的错误
    async fn write_value(&mut self, frame: &Frame) -> io::Result<()> {
        // 根据 Frame 结构体的不同类型，进行不同的处理
        match frame {
            // 如果是简单字符串类型，则先写入一个 '+' 字符，然后写入字符串的值，最后写入 "\r\n" 表示行结束
            Frame::Simple(val) => {
                self.stream.write_u8(b'+').await?;
                self.stream.write_all(val.as_bytes()).await?;
                self.stream.write_all(b"\r\n").await?;
            }
            // 如果是错误类型，则先写入一个 '-' 字符，然后写入错误字符串的值，最后写入 "\r\n" 表示行结束
            Frame::Error(val) => {
                self.stream.write_u8(b'-').await?;
                self.stream.write_all(val.as_bytes()).await?;
                self.stream.write_all(b"\r\n").await?;
            }
            // 如果是整数类型，则先写入一个 ':' 字符，然后写入整数的值
            Frame::Integer(val) => {
                self.stream.write_u8(b':').await?;
                self.write_decimal(*val).await?;
            }
            // 如果是批量类型，则先写入一个 '$' 字符，然后写入批量数据的长度，接着写入批量数据的值，最后写入 "\r\n" 表示行结束
            Frame::Bulk(val) => {
                let len = val.len();
                self.stream.write_u8(b'$').await?;
                self.write_decimal(len as u64).await?;
                self.stream.write_all(val).await?;
                self.stream.write_all(b"\r\n").await?;
            }
            // 如果是空类型，则写入 "-1\r\n" 表示空值
            Frame::Null => {
                self.stream.write_all(b"$-1\r\n").await?;
            }
            // 如果是数组类型，则不可能到达这里，因为在调用 write_value 之前已经进行了类型检查
            Frame::Array(_) => unreachable!(),
        }
        // 返回成功
        Ok(())
    }
    /// 将一个 `u64` 类型的十进制数写入到 `Connection` 结构体的缓冲区中
    /// 如果写入成功，则返回 `Ok(())`；如果写入失败，则返回相应的错误
    async fn write_decimal(&mut self, val: u64) -> io::Result<()> {
        use std::io::Write;
        // 创建一个长度为 12 的字节数组，用于存储十进制数的字符串表示
        let mut buf = [0u8; 12];
        // 创建一个 Cursor 对象，用于从字节数组中读取数据
        let mut buf = Cursor::new(&mut buf[..]);
        // 使用 write! 宏将十进制数转换为字符串，并写入到 Cursor 对象中
        write!(&mut buf, "{}", val)?;
        // 获取当前 Cursor 对象的位置，即已经写入的数据长度
        let pos = buf.position() as usize;
        // 将字节数组中从开始到当前位置的数据写入到连接的缓冲区中
        self.stream.write_all(&buf.get_ref()[..pos]).await?;
        // 写入 "\r\n" 表示行结束
        self.stream.write_all(b"\r\n").await?;
        // 返回成功
        Ok(())
    }
}
