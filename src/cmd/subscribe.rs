#[derive(Debug)]
pub struct Subscribe {
    channels: Vec<String>,
}

#[derive(Debug)]
pub struct Unsubscribe {
    channels: Vec<String>,
}
