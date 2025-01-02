use bytes::{Buf, Bytes};
use std::convert::{Infallible, TryInto};
use std::fmt;
use std::io::{Cursor};
use std::num::TryFromIntError;
use std::string::FromUtf8Error;

#[derive(Clone, Debug)]
pub enum Frame {
    Simple(String),
    Error(String),
    Integer(u64),
    Bulk(Bytes),
    Null,
    Array(Vec<Frame>),
}

#[derive(Debug)]
pub enum Error {
    Incomplete,
    Other(crate::Error),
}
impl From<String> for Error {
    fn from(e: String) -> Error {
        Error::Other(e.into())
    }
}

impl From<&str> for Error {
    fn from(s: &str) -> Error {
        Error::Other(s.into())
    }
}

impl From<FromUtf8Error> for Error {
    fn from(_: FromUtf8Error) -> Error {
        "protocol error; invalid frame format".into()
    }
}

impl From<TryFromIntError> for Error {
    fn from(_: TryFromIntError) -> Error {
        "protocol error; invalid frame format".into()
    }
}

impl From<Infallible> for Error {
    fn from(_: Infallible) -> Error {
        "protocol error; invalid frame format".into()
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Error::Incomplete => "stream ended early".fmt(f),
            Error::Other(err) => err.fmt(f),
        }
    }
}

impl std::error::Error for Error {}

impl Frame {
    pub(crate) fn array() -> Frame {
        Frame::Array(vec![])
    }

    pub(crate) fn push_bulk(&mut self, bytes: Bytes) {
        match self {
            Frame::Array(vec) => {
                vec.push(Frame::Bulk(bytes));
            }
            _ => panic!("not an array frame"),
        }
    }

    pub(crate) fn push_int(&mut self, value: u64) {
        match self {
            Frame::Array(vec) => {
                vec.push(Frame::Integer(value));
            }
            _ => panic!("not an array frame"),
        }
    }

    /// 检查 `Cursor<&[u8]>` 中的数据是否符合特定的协议格式
    /// 如果数据格式正确，则返回 `Ok(())`；如果数据格式不正确，则返回相应的错误
    pub fn check(src: &mut Cursor<&[u8]>) -> Result<(), Error> {
        // 读取下一个字节，并根据字节值进行不同的处理
        match get_u8(src)? {
            // 如果是 '+'，则读取下一行数据
            b'+' => {
                get_line(src)?;
                Ok(())
            }
            // 如果是 '-'，则读取下一行数据
            b'-' => {
                get_line(src)?;
                Ok(())
            }
            // 如果是 ':', 则读取一个十进制数
            b':' => {
                let _ = get_decimal(src)?;
                Ok(())
            }
            // 如果是 '$'，则根据下一个字节的值进行不同的处理
            b'$' => {
                // 如果下一个字节是 '-'，则跳过 4 个字节
                if b'-' == peek_u8(src)? {
                    skip(src, 4)
                // 如果下一个字节不是 '-'，则读取一个十进制数，并跳过相应数量的字节
                } else {
                    let len = get_decimal(src)?.try_into()?;
                    skip(src, len + 2)
                }
            }
            // 如果是 '*'，则读取一个十进制数，并对每个值进行检查
            b'*' => {
                let len = get_decimal(src)?;
                for _ in 0..len {
                    Frame::check(src)?;
                }
                Ok(())
            }
            // 如果是其他字节，则返回错误
            actual => Err(format!("protocol error;invalid frame type byte `{}`", actual).into()),
        }
    }

    /// 从 `Cursor<&[u8]>` 中解析出一个 `Frame` 结构体
    /// 如果解析成功，则返回 `Ok(Frame)`；如果解析失败，则返回相应的错误
    pub fn parse(src: &mut Cursor<&[u8]>) -> Result<Frame, Error> {
        // 读取下一个字节，并根据字节值进行不同的处理
        match get_u8(src)? {
            // 如果是 '+'，则读取下一行数据，并将其解析为一个简单字符串帧
            b'+' => {
                let line = get_line(src)?.to_vec();
                let string = String::from_utf8(line)?;
                Ok(Frame::Simple(string))
            }
            // 如果是 '-'，则读取下一行数据，并将其解析为一个错误帧
            b'-' => {
                let line = get_line(src)?.to_vec();
                let string = String::from_utf8(line)?;
                Ok(Frame::Error(string))
            }
            // 如果是 ':', 则读取一个十进制数，并将其解析为一个整数帧
            b':' => {
                let len = get_decimal(src)?;
                Ok(Frame::Integer(len))
            }
            // 如果是 '$'，则根据下一个字节的值进行不同的处理
            b'$' => {
                // 如果下一个字节是 '-'，则读取下一行数据，并将其解析为空帧
                if b'-' == peek_u8(src)? {
                    let line = get_line(src)?;
                    if line != b"-1" {
                        return Err(format!(
                            "protocol error;invalid bulk frame `{}`",
                            String::from_utf8(line.to_vec())?
                        )
                        .into());
                    }
                    Ok(Frame::Null)
                // 如果下一个字节不是 '-'，则读取一个十进制数，并跳过相应数量的字节，然后将数据解析为一个批量帧
                } else {
                    let len = get_decimal(src)?.try_into()?;
                    let n = len + 2;
                    if src.remaining() < n {
                        return Err(Error::Incomplete);
                    }
                    let data = Bytes::copy_from_slice(&src.bytes()[..len]);
                    skip(src, n)?;
                    Ok(Frame::Bulk(data))
                }
            }
            // 如果是 '*'，则读取一个十进制数，并对每个值进行解析，然后将这些帧组合成一个数组帧
            b'*' => {
                let len = get_decimal(src)?.try_into()?;
                let mut out = Vec::with_capacity(len);
                for _ in 0..len {
                    out.push(Frame::parse(src)?);
                }
                Ok(Frame::Array(out))
            }
            // 如果是其他字节，则返回错误
            _ => unimplemented!(),
        }
    }

    pub(crate) fn to_error(&self) -> crate::Error {
        format!("unexpected frame:{}", self).into()
    }
}

impl PartialEq<&str> for Frame {
    fn eq(&self, other: &&str) -> bool {
        match self {
            Frame::Simple(s) => s.eq(other),
            Frame::Bulk(s) => s.eq(other),
            _ => false,
        }
    }
}
impl fmt::Display for Frame {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use std::str;
        // 根据 Frame 枚举类型的不同变体，格式化输出不同的字符串
        match self {
            // 如果是 Simple 变体，直接格式化字符串
            Frame::Simple(response) => response.fmt(f),
            // 如果是 Error 变体，输出 "error:" 前缀，然后格式化错误消息
            Frame::Error(msg) => write!(f, "error:{}", msg),
            // 如果是 Integer 变体，直接格式化整数
            Frame::Integer(num) => num.fmt(f),
            // 如果是 Bulk 变体，尝试将字节数组转换为 UTF-8 字符串，如果成功则格式化字符串，否则输出字节数组的十六进制表示
            Frame::Bulk(msg) => match str::from_utf8(msg) {
                Ok(string) => string.fmt(f),
                Err(_) => write!(f, "{:x?}", msg),
            },
            // 如果是 Null 变体，输出 "(nil)"
            Frame::Null => "(nil)".fmt(f),
            // 如果是 Array 变体，遍历数组中的每个元素，用空格分隔，然后格式化每个元素
            Frame::Array(parts) => {
                for (i, part) in parts.iter().enumerate() {
                    if i > 0 {
                        write!(f, " ")?;
                        part.fmt(f)?;
                    }
                }
                Ok(())
            }
        }
    }
}

/// 从 `Cursor<&[u8]>` 中读取下一个字节，但不移动光标位置
/// 如果数据源中没有剩余字节，则返回 `Error::Incomplete`
fn peek_u8(src: &mut Cursor<&[u8]>) -> Result<u8, Error> {
    // 检查是否还有剩余字节
    if !src.has_remaining() {
        // 如果没有剩余字节，返回 `Incomplete` 错误
        return Err(Error::Incomplete);
    }
    // 返回下一个字节
    Ok(src.bytes()[0])
}

/// 从 `Cursor<&[u8]>` 中读取下一个字节
/// 如果数据源中没有剩余字节，则返回 `Error::Incomplete`
fn get_u8(src: &mut Cursor<&[u8]>) -> Result<u8, Error> {
    // 检查是否还有剩余字节
    if !src.has_remaining() {
        // 如果没有剩余字节，返回 `Incomplete` 错误
        return Err(Error::Incomplete);
    }
    // 返回下一个字节
    Ok(src.get_u8())
}
/// 从 `Cursor<&[u8]>` 中跳过指定数量的字节
/// 如果数据源中没有足够的字节，则返回 `Error::Incomplete`
fn skip(src: &mut Cursor<&[u8]>, n: usize) -> Result<(), Error> {
    // 检查是否还有足够的剩余字节
    if src.remaining() < n {
        // 如果没有足够的剩余字节，返回 `Incomplete` 错误
        return Err(Error::Incomplete);
    }
    // 跳过指定数量的字节
    src.advance(n);
    // 返回成功
    Ok(())
}

/// 从 `Cursor<&[u8]>` 中读取下一行，并将其解析为一个 `u64` 类型的十进制数
/// 如果数据源中没有剩余字节或者解析的数字格式不正确，则返回 `Error`
fn get_decimal(src: &mut Cursor<&[u8]>) -> Result<u64, Error> {
    use atoi::atoi;
    // 读取下一行数据
    let line = get_line(src)?;
    // 将读取到的字符串解析为 u64 类型的数字
    atoi::<u64>(line).ok_or_else(|| "protocol error; invalid decimal number".into())
}
/// 从 `Cursor<&[u8]>` 中读取下一行数据
/// 如果数据源中没有剩余字节或者没有找到行结束符，则返回 `Error::Incomplete`
fn get_line<'a>(src: &mut Cursor<&'a [u8]>) -> Result<&'a [u8], Error> {
    // 获取当前光标位置作为行的起始位置
    let start = src.position() as usize;
    // 获取数据源的长度作为行的结束位置
    let end = src.get_ref().len() - 1;

    // 从当前位置开始遍历数据源，查找行结束符 '\r\n'
    for i in start..end {
        // 如果当前字节是 '\r'，并且下一个字节是 '\n'，则找到了行结束符
        if src.get_ref()[i] == b'\r' && src.get_ref()[i + 1] == b'\n' {
            // 将光标位置移动到行结束符之后的下一个字节
            src.set_position((i + 2) as u64);
            // 返回从起始位置到行结束符之前的字节切片
            return Ok(&src.get_ref()[start..i]);
        }
    }
    // 如果没有找到行结束符，则返回 `Incomplete` 错误
    Err(Error::Incomplete)
}
