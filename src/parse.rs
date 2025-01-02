use std::{fmt, vec};
use bytes::Bytes;
use crate::frame::Frame;

#[derive(Debug)]
pub(crate) struct Parse {
    parts:vec::IntoIter<Frame>,
}

#[derive(Debug)]
pub(crate) enum ParseError {
    EndOfStream,
    Other(crate::Error),
}

impl Parse{
    pub(crate)  fn new(frame:Frame)->Result<Parse,ParseError>{
        let array = match frame{
            Frame::Array(parts)=>parts,
            frame=>return Err(format!("protocol error; expected array, got {:?}", frame).into()),
        };
        Ok(Parse{
            parts:array.into_iter(),
        })
    }

    fn next(&mut self)->Result<Frame,ParseError>{
        self.parts.next().ok_or(ParseError::EndOfStream)
    }

    pub(crate) fn next_string(&mut self)->Result<String,ParseError>{
        match self.next()?{
            Frame::Simple(s) => Ok(s),
            Frame::Bulk(data) => std::str::from_utf8(&data[..])
                .map(|s| s.to_string())
                .map_err(|_| "protocol error; expected string".into()),
            frame=>Err(format!("protocol error; expected simple frame or bulk frame, got {:?}", frame).into()),
        }
    }

    pub(crate) fn next_bytes(&mut self)->Result<Bytes,ParseError>{
        match self.next()?{
            Frame::Simple(s) => Ok(Bytes::from(s.into_bytes())),
            Frame::Bulk(data)=>Ok(data),
            frame => Err(format!("protocol error; expected simple frame or bulk frame, got {:?}", frame).into()),
        }
    }

    pub(crate) fn next_int(&mut self)->Result<u64,ParseError>{
        use atoi::atoi;
        const MSG:&str = "protocol error; expected number";
        match self.next()? {
            Frame::Simple(data) => atoi::<u64>(data.as_bytes()).ok_or_else(||MSG.into()),
            Frame::Integer(v) => Ok(v),
            Frame::Bulk(data) => atoi::<u64>(&data).ok_or_else(||MSG.into()),
            frame=>Err(format!("protocol error; expected int frame, got {:?}", frame).into()),
        }
    }

    pub(crate)  fn finish(&mut self)->Result<(),ParseError>{
        if self.parts.next().is_none(){
            Ok(())
        }else{
            Err("protocol error; expected end of array".into())
        }
    }
}

impl From<String> for ParseError{
    fn from(e:String)->ParseError{
        ParseError::Other(e.into())
    }
}

impl From<&str> for ParseError {
    fn from(s: &str) -> ParseError {
        ParseError::Other(s.into())
    }
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ParseError::EndOfStream => "protocol error,unexpected end of stream".fmt(f),
            ParseError::Other(err) => err.fmt(f),
        }
    }
}
impl std::error::Error for ParseError {}