use thiserror::Error;

#[derive(Error, Debug)]
pub enum SessionError {
    // #[error("Session cash broken")]
    // SessionCashBroken,
    #[error("Session not found")]
    SessionNotFound,
    #[error("Session was updated")]
    SessionWasUpdated,
}