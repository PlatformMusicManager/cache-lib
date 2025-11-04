use thiserror::Error;

#[derive(Error, Debug)]
pub enum UserVerifyError {
    #[error("Fale to parse entity from db")]
    ParseError,
    #[error("User not found, ")]
    UserNotFound,
    #[error("Exceeded amount of attempts")]
    ExceededAttempts,
    #[error("Wrong code")]
    WrongCode,
    #[error("Expired")]
    Expired,
}