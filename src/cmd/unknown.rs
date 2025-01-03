use crate::connection::Connection;
use crate::frame::Frame;
use tracing::debug;

#[derive(Debug)]
pub struct Unknown {
    command_name: String,
}

impl Unknown {
    pub(crate) fn new(command_name: impl ToString) -> Unknown {
        Unknown {
            command_name: command_name.to_string(),
        }
    }

    pub(crate) fn get_name(&self) -> &str {
        self.command_name.as_str()
    }

    pub(crate) async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        let response = Frame::Error(format!("ERR unknown command '{}'", self.command_name));
        debug!(?response);
        let _ = dst.write_frame(&response).await;
        Ok(())
    }
}
