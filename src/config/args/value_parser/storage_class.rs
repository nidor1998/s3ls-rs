use aws_sdk_s3::types::StorageClass;

const INVALID_STORAGE_CLASS: &str = "invalid storage class. valid choices: STANDARD | REDUCED_REDUNDANCY | STANDARD_IA | ONE-ZONE_IA | INTELLIGENT_TIERING | GLACIER | DEEP_ARCHIVE | GLACIER_IR | EXPRESS_ONEZONE.";

pub fn parse_storage_class(class: &str) -> Result<String, String> {
    #[allow(deprecated)]
    if matches!(StorageClass::from(class), StorageClass::Unknown(_)) {
        return Err(INVALID_STORAGE_CLASS.to_string());
    }

    Ok(class.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn valid_storage_classes_accepted() {
        let valid = [
            "STANDARD",
            "REDUCED_REDUNDANCY",
            "STANDARD_IA",
            "ONEZONE_IA",
            "INTELLIGENT_TIERING",
            "GLACIER",
            "DEEP_ARCHIVE",
            "GLACIER_IR",
            "EXPRESS_ONEZONE",
        ];
        for class in valid {
            assert_eq!(
                parse_storage_class(class),
                Ok(class.to_string()),
                "expected {class} to be accepted"
            );
        }
    }

    #[test]
    fn invalid_storage_class_rejected() {
        let invalid = ["INVALID", "standard", "glacier", "", "WARM", "S3_STANDARD"];
        for class in invalid {
            let result = parse_storage_class(class);
            assert!(
                result.is_err(),
                "expected {class:?} to be rejected, but got Ok({:?})",
                result.unwrap()
            );
            assert!(
                result.unwrap_err().contains("invalid storage class"),
                "error message should mention 'invalid storage class'"
            );
        }
    }
}
