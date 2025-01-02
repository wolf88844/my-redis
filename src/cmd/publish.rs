use bytes::Bytes;

#[derive(Debug)]
pub struct Publish {
    channel: String,
    message: Bytes,
}
