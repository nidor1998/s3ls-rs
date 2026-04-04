use url::Url;

const INVALID_SCHEME: &str = "URL scheme must be https:// or http://";

pub fn check_scheme(url: &str) -> Result<String, String> {
    let parsed = Url::parse(url).map_err(|e| e.to_string())?;
    if parsed.scheme() != "https" && parsed.scheme() != "http" {
        return Err(INVALID_SCHEME.to_string());
    }
    Ok(url.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn check_scheme_accepts_https() {
        let result = check_scheme("https://example.com");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "https://example.com");
    }

    #[test]
    fn check_scheme_accepts_http() {
        let result = check_scheme("http://localhost:9000");
        assert!(result.is_ok());
    }

    #[test]
    fn check_scheme_rejects_ftp() {
        let result = check_scheme("ftp://files.example.com");
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "URL scheme must be https:// or http://");
    }

    #[test]
    fn check_scheme_rejects_file_scheme() {
        let result = check_scheme("file:///tmp/test");
        assert!(result.is_err());
    }

    #[test]
    fn check_scheme_rejects_invalid_url() {
        let result = check_scheme("not-a-url");
        assert!(result.is_err());
    }
}
