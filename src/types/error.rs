use thiserror::Error;

#[derive(Error, Debug)]
pub enum S3lsError {
    #[error("Invalid URI: {0}")]
    InvalidUri(String),

    #[error("Invalid configuration: {0}")]
    InvalidConfig(String),

    #[error("Listing error: {0}")]
    ListingError(String),

    #[error("Pipeline cancelled")]
    Cancelled,
}

impl S3lsError {
    pub fn exit_code(&self) -> i32 {
        match self {
            S3lsError::Cancelled => 0,
            S3lsError::InvalidConfig(_) | S3lsError::InvalidUri(_) => 2,
            S3lsError::ListingError(_) => 1,
        }
    }
}

pub fn is_cancelled_error(err: &anyhow::Error) -> bool {
    err.downcast_ref::<S3lsError>()
        .is_some_and(|e| matches!(e, S3lsError::Cancelled))
}

pub fn exit_code_from_error(err: &anyhow::Error) -> i32 {
    err.downcast_ref::<S3lsError>()
        .map(|e| e.exit_code())
        .unwrap_or(1)
}
