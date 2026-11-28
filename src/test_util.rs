//! Test-only helpers shared by the unit tests.
//!
//! [`ReplayHttpClient`] is a minimal HTTP client for the AWS SDK that hands
//! out canned responses in order and records the URI of every request it
//! receives, so code that takes a real `aws_sdk_s3::Client` can be exercised
//! without a network.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use aws_sdk_s3::Client;
use aws_sdk_s3::config::retry::RetryConfig;
use aws_sdk_s3::config::{BehaviorVersion, Credentials, Region};
use aws_smithy_runtime_api::client::http::{
    HttpClient, HttpConnector, HttpConnectorFuture, HttpConnectorSettings, SharedHttpConnector,
};
use aws_smithy_runtime_api::client::orchestrator::{HttpRequest, HttpResponse};
use aws_smithy_runtime_api::client::runtime_components::RuntimeComponents;
use aws_smithy_runtime_api::http::StatusCode;
use aws_smithy_types::body::SdkBody;

#[derive(Clone, Debug)]
pub(crate) struct ReplayHttpClient {
    responses: Arc<Mutex<VecDeque<(u16, String)>>>,
    request_uris: Arc<Mutex<Vec<String>>>,
}

impl ReplayHttpClient {
    /// Create a client that answers successive requests with the given
    /// `(status, body)` pairs. A request beyond the last pair panics.
    pub(crate) fn new(responses: Vec<(u16, &str)>) -> Self {
        Self {
            responses: Arc::new(Mutex::new(
                responses
                    .into_iter()
                    .map(|(status, body)| (status, body.to_string()))
                    .collect(),
            )),
            request_uris: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// An S3 client that sends every request to this replay client. Retries
    /// are disabled so one canned response answers exactly one API call.
    pub(crate) fn s3_client(&self) -> Client {
        let config = aws_sdk_s3::Config::builder()
            .behavior_version(BehaviorVersion::latest())
            .region(Region::new("us-east-1"))
            .credentials_provider(Credentials::new(
                "test-access-key",
                "test-secret-key",
                None,
                None,
                "test",
            ))
            .retry_config(RetryConfig::disabled())
            .http_client(self.clone())
            .build();
        Client::from_conf(config)
    }

    /// URIs of the requests received so far, in order.
    pub(crate) fn request_uris(&self) -> Vec<String> {
        self.request_uris.lock().unwrap().clone()
    }
}

impl HttpConnector for ReplayHttpClient {
    fn call(&self, request: HttpRequest) -> HttpConnectorFuture {
        self.request_uris
            .lock()
            .unwrap()
            .push(request.uri().to_string());
        let (status, body) = self
            .responses
            .lock()
            .unwrap()
            .pop_front()
            .expect("ReplayHttpClient received more requests than canned responses");
        HttpConnectorFuture::ready(Ok(HttpResponse::new(
            StatusCode::try_from(status).expect("valid HTTP status code"),
            SdkBody::from(body),
        )))
    }
}

impl HttpClient for ReplayHttpClient {
    fn http_connector(
        &self,
        _settings: &HttpConnectorSettings,
        _components: &RuntimeComponents,
    ) -> SharedHttpConnector {
        SharedHttpConnector::new(self.clone())
    }
}
