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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exit_code_cancelled_is_zero() {
        assert_eq!(S3lsError::Cancelled.exit_code(), 0);
    }

    #[test]
    fn exit_code_invalid_config_is_two() {
        assert_eq!(S3lsError::InvalidConfig("bad".into()).exit_code(), 2);
    }

    #[test]
    fn exit_code_invalid_uri_is_two() {
        assert_eq!(S3lsError::InvalidUri("bad".into()).exit_code(), 2);
    }

    #[test]
    fn exit_code_listing_error_is_one() {
        assert_eq!(S3lsError::ListingError("fail".into()).exit_code(), 1);
    }

    #[test]
    fn is_cancelled_error_true_for_cancelled() {
        let err: anyhow::Error = S3lsError::Cancelled.into();
        assert!(is_cancelled_error(&err));
    }

    #[test]
    fn is_cancelled_error_false_for_other_variants() {
        let err: anyhow::Error = S3lsError::ListingError("x".into()).into();
        assert!(!is_cancelled_error(&err));
    }

    #[test]
    fn is_cancelled_error_false_for_non_s3ls_error() {
        let err = anyhow::anyhow!("generic error");
        assert!(!is_cancelled_error(&err));
    }

    #[test]
    fn exit_code_from_error_delegates_to_variant() {
        let err: anyhow::Error = S3lsError::InvalidUri("bad".into()).into();
        assert_eq!(exit_code_from_error(&err), 2);
    }

    #[test]
    fn exit_code_from_error_defaults_to_one_for_unknown() {
        let err = anyhow::anyhow!("unknown error");
        assert_eq!(exit_code_from_error(&err), 1);
    }
}
