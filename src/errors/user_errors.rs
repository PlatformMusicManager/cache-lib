use serde_json::error::Error;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum UserError {
    #[error("Fale to parse entity from db")]
    ParseError,
    #[error("User not found")]
    UserNotFound,
}
