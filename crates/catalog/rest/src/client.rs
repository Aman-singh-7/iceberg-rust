// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::time::{Duration, Instant};

use http::StatusCode;
use iceberg::{Error, ErrorKind, Result};
use reqwest::header::HeaderMap;
use reqwest::{Client, IntoUrl, Method, Request, RequestBuilder, Response};
use serde::de::DeserializeOwned;
use tokio::sync::Mutex;
use tracing::Instrument;

use crate::RestCatalogConfig;
use crate::trace::{current_trace_id, traceparent};
use crate::types::{ErrorResponse, TokenResponse};

/// Refresh the token slightly before its server-side expiry so an in-flight
/// request can't cross the boundary and arrive with an already-expired token.
const TOKEN_EXPIRY_SAFETY_MARGIN: Duration = Duration::from_secs(30);

/// A cached OAuth token together with its absolute expiry deadline, if known.
///
/// Tokens supplied directly via configuration have no `expires_in` and are
/// treated as never-expiring from this client's perspective. Tokens minted by
/// exchanging a credential carry the deadline derived from the server's
/// `expires_in` (minus a safety margin).
#[derive(Debug, Clone)]
struct CachedToken {
    token: String,
    expires_at: Option<Instant>,
}

impl CachedToken {
    fn new(token: String, expires_in: Option<u64>) -> Self {
        let expires_at = expires_in.map(|secs| {
            let lifetime = Duration::from_secs(secs).saturating_sub(TOKEN_EXPIRY_SAFETY_MARGIN);
            Instant::now() + lifetime
        });
        Self { token, expires_at }
    }

    fn from_static(token: String) -> Self {
        Self {
            token,
            expires_at: None,
        }
    }

    fn is_expired(&self) -> bool {
        self.expires_at.is_some_and(|at| Instant::now() >= at)
    }
}

pub(crate) struct HttpClient {
    client: Client,

    /// The token to be used for authentication.
    ///
    /// It's possible to fetch the token from the server while needed.
    token: Mutex<Option<CachedToken>>,
    /// The token endpoint to be used for authentication.
    token_endpoint: String,
    /// The credential to be used for authentication.
    credential: Option<(Option<String>, String)>,
    /// Extra headers to be added to each request.
    extra_headers: HeaderMap,
    /// Extra oauth parameters to be added to each authentication request.
    extra_oauth_params: HashMap<String, String>,
    /// Whether to disable header redaction in error logs (defaults to false for security).
    disable_header_redaction: bool,
}

impl Debug for HttpClient {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpClient")
            .field("client", &self.client)
            .field("extra_headers", &self.extra_headers)
            .finish_non_exhaustive()
    }
}

impl HttpClient {
    /// Create a new http client.
    pub fn new(cfg: &RestCatalogConfig) -> Result<Self> {
        let extra_headers = cfg.extra_headers()?;
        Ok(HttpClient {
            client: cfg.client().unwrap_or_default(),
            token: Mutex::new(cfg.token().map(CachedToken::from_static)),
            token_endpoint: cfg.get_token_endpoint(),
            credential: cfg.credential(),
            extra_headers,
            extra_oauth_params: cfg.extra_oauth_params(),
            disable_header_redaction: cfg.disable_header_redaction(),
        })
    }

    /// Update the http client with new configuration.
    ///
    /// If cfg carries new value, we will use cfg instead.
    /// Otherwise, we will keep the old value.
    pub fn update_with(self, cfg: &RestCatalogConfig) -> Result<Self> {
        let extra_headers = (!cfg.extra_headers()?.is_empty())
            .then(|| cfg.extra_headers())
            .transpose()?
            .unwrap_or(self.extra_headers);
        Ok(HttpClient {
            client: cfg.client().unwrap_or(self.client),
            token: Mutex::new(
                cfg.token()
                    .map(CachedToken::from_static)
                    .or_else(|| self.token.into_inner()),
            ),
            token_endpoint: if !cfg.get_token_endpoint().is_empty() {
                cfg.get_token_endpoint()
            } else {
                self.token_endpoint
            },
            credential: cfg.credential().or(self.credential),
            extra_headers,
            extra_oauth_params: if !cfg.extra_oauth_params().is_empty() {
                cfg.extra_oauth_params()
            } else {
                self.extra_oauth_params
            },
            disable_header_redaction: cfg.disable_header_redaction(),
        })
    }

    /// This API is testing only to assert the token.
    #[cfg(test)]
    pub(crate) async fn token(&self) -> Option<String> {
        let mut req = self
            .request(Method::GET, &self.token_endpoint)
            .build()
            .unwrap();
        self.authenticate(&mut req).await.ok();
        self.token.lock().await.as_ref().map(|c| c.token.clone())
    }

    async fn exchange_credential_for_token(&self) -> Result<CachedToken> {
        // Credential must exist here.
        let (client_id, client_secret) = self.credential.as_ref().ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                "Credential must be provided for authentication",
            )
        })?;

        let mut params = HashMap::with_capacity(4);
        params.insert("grant_type", "client_credentials");
        if let Some(client_id) = client_id {
            params.insert("client_id", client_id);
        }
        params.insert("client_secret", client_secret);
        params.extend(
            self.extra_oauth_params
                .iter()
                .map(|(k, v)| (k.as_str(), v.as_str())),
        );

        let mut auth_req = self
            .request(Method::POST, &self.token_endpoint)
            .form(&params)
            .build()?;
        // extra headers add content-type application/json header it's necessary to override it with proper type
        // note that form call doesn't add content-type header if already present
        auth_req.headers_mut().insert(
            http::header::CONTENT_TYPE,
            http::HeaderValue::from_static("application/x-www-form-urlencoded"),
        );
        let auth_url = auth_req.url().clone();
        let auth_resp = send_traced(&self.client, auth_req).await?;

        let auth_res: TokenResponse = if auth_resp.status() == StatusCode::OK {
            let text = auth_resp
                .bytes()
                .await
                .map_err(|err| err.with_url(auth_url.clone()))?;
            Ok(serde_json::from_slice(&text).map_err(|e| {
                Error::new(
                    ErrorKind::Unexpected,
                    "Failed to parse response from rest catalog server!",
                )
                .with_context("operation", "auth")
                .with_context("url", auth_url.to_string())
                .with_context("json", String::from_utf8_lossy(&text))
                .with_source(e)
            })?)
        } else {
            let code = auth_resp.status();
            let text = auth_resp
                .bytes()
                .await
                .map_err(|err| err.with_url(auth_url.clone()))?;
            let e: ErrorResponse = serde_json::from_slice(&text).map_err(|e| {
                Error::new(ErrorKind::Unexpected, "Received unexpected response")
                    .with_context("code", code.to_string())
                    .with_context("operation", "auth")
                    .with_context("url", auth_url.to_string())
                    .with_context("json", String::from_utf8_lossy(&text))
                    .with_source(e)
            })?;
            Err(Error::from(e))
        }?;
        Ok(CachedToken::new(auth_res.access_token, auth_res.expires_in))
    }

    /// Invalidate the current token without generating a new one. On the next request, the client
    /// will attempt to generate a new token.
    pub(crate) async fn invalidate_token(&self) -> Result<()> {
        *self.token.lock().await = None;
        Ok(())
    }

    /// Invalidate the current token and set a new one. Generates a new token before invalidating
    /// the current token, meaning the old token will be used until this function acquires the lock
    /// and overwrites the token.
    ///
    /// If credential is invalid, or the request fails, this method will return an error and leave
    /// the current token unchanged.
    pub(crate) async fn regenerate_token(&self) -> Result<()> {
        let new_token = self.exchange_credential_for_token().await?;
        *self.token.lock().await = Some(new_token);
        Ok(())
    }

    /// Force-refreshes the OAuth token after a request came back `401 Unauthorized`.
    ///
    /// Proactive, deadline-based refresh in [`authenticate`] handles tokens that
    /// expire on schedule, but the server can reject a token we still believe is
    /// valid — e.g. clock skew, or a Polaris restart that wiped its session state.
    /// This is the reactive safety net: mint a fresh token and report it so the
    /// caller can replay the failed request once.
    ///
    /// `stale_token` is the bearer that just failed. Concurrent 401s coalesce
    /// through the token mutex: the first holder regenerates, and any task that
    /// finds the cache already advanced past its own stale token reuses the new
    /// one instead of triggering another exchange.
    ///
    /// Returns `Ok(None)` when there is no credential to refresh with (static
    /// token or unauthenticated client), leaving the original 401 to propagate.
    async fn refresh_after_unauthorized(
        &self,
        stale_token: Option<&str>,
    ) -> Result<Option<String>> {
        if self.credential.is_none() {
            return Ok(None);
        }

        let mut guard = self.token.lock().await;
        if let (Some(cached), Some(stale)) = (guard.as_ref(), stale_token)
            && cached.token != stale
        {
            // Another task already refreshed while we waited on the lock.
            return Ok(Some(cached.token.clone()));
        }

        let new = self.exchange_credential_for_token().await?;
        let token = new.token.clone();
        *guard = Some(new);
        Ok(Some(token))
    }

    /// Authenticates the request by adding a bearer token to the authorization header.
    ///
    /// This method supports three authentication modes:
    ///
    /// 1. **No authentication** - Skip authentication when both `credential` and `token` are missing.
    /// 2. **Token authentication** - Use the provided `token` directly for authentication.
    /// 3. **OAuth authentication** - Exchange `credential` for a token, cache it, then use it for authentication.
    ///    When the cached token's `expires_in` deadline has passed, it is refreshed automatically.
    ///
    /// When both `credential` and `token` are present, `token` takes precedence.
    async fn authenticate(&self, req: &mut Request) -> Result<()> {
        let cached = self.token.lock().await.clone();

        if self.credential.is_none() && cached.is_none() {
            return Ok(());
        }

        let token = match cached {
            Some(c) if !c.is_expired() => c.token,
            _ => {
                if self.credential.is_some() {
                    // Hold the lock across the exchange so concurrent callers
                    // don't all stampede the token endpoint. Re-check after
                    // acquisition in case another task has just refreshed.
                    let mut guard = self.token.lock().await;
                    match guard.as_ref() {
                        Some(c) if !c.is_expired() => c.token.clone(),
                        _ => {
                            let new = self.exchange_credential_for_token().await?;
                            let token = new.token.clone();
                            *guard = Some(new);
                            token
                        }
                    }
                } else {
                    // Static token with no credential — nothing to refresh
                    // with. Pass it through; the server will reject if it has
                    // truly expired (preserves prior behavior).
                    cached
                        .expect("cached must be Some when credential is None")
                        .token
                }
            }
        };

        req.headers_mut().insert(
            http::header::AUTHORIZATION,
            format!("Bearer {token}").parse().map_err(|e| {
                Error::new(
                    ErrorKind::DataInvalid,
                    "Invalid token received from catalog server!",
                )
                .with_source(e)
            })?,
        );

        Ok(())
    }

    #[inline]
    pub fn request<U: IntoUrl>(&self, method: Method, url: U) -> RequestBuilder {
        self.client
            .request(method, url)
            .headers(self.extra_headers.clone())
    }

    /// Executes the given `Request` and returns a `Response`.
    pub async fn execute(&self, mut request: Request) -> Result<Response> {
        request.headers_mut().extend(self.extra_headers.clone());
        Ok(send_traced(&self.client, request).await?)
    }

    // Queries the Iceberg REST catalog after authentication with the given `Request` and
    // returns a `Response`.
    //
    // On `401 Unauthorized` the token we used is force-refreshed and the request
    // is re-issued exactly once with fresh credentials. This recovers from a
    // server-side token invalidation (clock skew, Polaris restart) that the
    // proactive expiry refresh in `authenticate` can't anticipate, so callers
    // no longer have to restart the process to recover.
    pub async fn query_catalog(&self, mut request: Request) -> Result<Response> {
        self.authenticate(&mut request).await?;

        // Snapshot the authenticated request before `execute` consumes it, so we
        // can replay it on a 401. Streaming bodies can't be cloned; when that
        // happens we simply forgo the retry and surface the original response.
        let retry_request = request.try_clone();

        let response = self.execute(request).await?;
        if response.status() != StatusCode::UNAUTHORIZED {
            return Ok(response);
        }

        let Some(mut retry_request) = retry_request else {
            return Ok(response);
        };

        // Recover the bearer we just sent so concurrent 401s coalesce onto a
        // single refresh.
        let stale_token = retry_request
            .headers()
            .get(http::header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.strip_prefix("Bearer "))
            .map(str::to_string);

        let Some(fresh_token) = self
            .refresh_after_unauthorized(stale_token.as_deref())
            .await?
        else {
            // No credential to refresh with — nothing more we can do.
            return Ok(response);
        };

        retry_request.headers_mut().insert(
            http::header::AUTHORIZATION,
            format!("Bearer {fresh_token}").parse().map_err(|e| {
                Error::new(
                    ErrorKind::DataInvalid,
                    "Invalid token received from catalog server!",
                )
                .with_source(e)
            })?,
        );

        self.execute(retry_request).await
    }

    /// Returns whether header redaction is disabled for this client.
    pub(crate) fn disable_header_redaction(&self) -> bool {
        self.disable_header_redaction
    }
}

/// Sends one request inside an `http` span, with a fresh `X-Request-ID` and, inside
/// `with_trace_id`, a `traceparent`. Field names match iceberg-catalog's own spans.
/// The span, and `elapsed_ms`, end when the response headers arrive; the body is
/// read outside it.
async fn send_traced(client: &Client, mut request: Request) -> reqwest::Result<Response> {
    let request_id = uuid::Uuid::new_v4();
    request.headers_mut().insert(
        "x-request-id",
        http::HeaderValue::from_str(&request_id.to_string()).expect("a UUID is plain ASCII"),
    );
    let trace = current_trace_id().map(|trace_id| (trace_id, uuid::Uuid::new_v4().as_u64_pair().1));
    if let Some((trace_id, span_id)) = trace {
        request.headers_mut().insert(
            "traceparent",
            http::HeaderValue::from_str(&traceparent(trace_id, span_id))
                .expect("hex is plain ASCII"),
        );
    }
    let span = tracing::debug_span!(
        "http",
        http.request.method = %request.method(),
        url.path = request.url().path(),
        server.address = request.url().host_str().unwrap_or_default(),
        polaris.request_id = %request_id,
        trace_id = tracing::field::Empty,
        span_id = tracing::field::Empty,
        http.response.status_code = tracing::field::Empty,
        elapsed_ms = tracing::field::Empty,
        error = tracing::field::Empty,
    );
    if let Some((trace_id, span_id)) = trace {
        span.record("trace_id", format_args!("{trace_id:032x}"));
        span.record("span_id", format_args!("{span_id:016x}"));
    }
    let started = Instant::now();
    let result = client.execute(request).instrument(span.clone()).await;
    match &result {
        Ok(response) => {
            span.record("http.response.status_code", response.status().as_u16());
        }
        Err(error) => {
            // As `dyn Error`, so the cause chain (e.g. connection refused) is recorded too.
            let error: &(dyn std::error::Error + 'static) = error;
            span.record("error", error);
        }
    }
    span.record("elapsed_ms", started.elapsed().as_millis() as u64);
    result
}

/// Deserializes a catalog response into the given [`DeserializedOwned`] type.
///
/// Returns an error if unable to parse the response bytes.
pub(crate) async fn deserialize_catalog_response<R: DeserializeOwned>(
    response: Response,
) -> Result<R> {
    let bytes = response.bytes().await?;

    serde_json::from_slice::<R>(&bytes).map_err(|e| {
        Error::new(
            ErrorKind::Unexpected,
            "Failed to parse response from rest catalog server",
        )
        .with_context("json", String::from_utf8_lossy(&bytes))
        .with_source(e)
    })
}

/// Headers that contain sensitive information and should be excluded from logs.
const SENSITIVE_HEADERS: &[&str] = &[
    "authorization",
    "proxy-authorization",
    "set-cookie",
    "cookie",
    "x-api-key",
    "x-auth-token",
];

/// Returns true if the header name is considered sensitive.
fn is_sensitive_header(name: &str) -> bool {
    let name_lower = name.to_lowercase();
    SENSITIVE_HEADERS.iter().any(|h| name_lower == *h)
}

/// Redacts sensitive headers and returns a debug-formatted string.
///
/// If `disable_redaction` is true, returns all headers without redaction.
/// Otherwise, replaces sensitive header values with "[REDACTED]".
fn format_headers_redacted(headers: &HeaderMap, disable_redaction: bool) -> String {
    if disable_redaction {
        // Return all headers as-is without redaction
        let all: HashMap<&str, &str> = headers
            .iter()
            .filter_map(|(name, value)| value.to_str().ok().map(|v| (name.as_str(), v)))
            .collect();
        return format!("{all:?}");
    }

    // Redact sensitive headers by replacing their values with "[REDACTED]"
    let redacted: HashMap<&str, &str> = headers
        .iter()
        .filter_map(|(name, value)| {
            if is_sensitive_header(name.as_str()) {
                Some((name.as_str(), "[REDACTED]"))
            } else {
                value.to_str().ok().map(|v| (name.as_str(), v))
            }
        })
        .collect();
    format!("{redacted:?}")
}

/// Deserializes a unexpected catalog response into an error.
pub(crate) async fn deserialize_unexpected_catalog_error(
    response: Response,
    disable_header_redaction: bool,
) -> Error {
    let err = Error::new(
        ErrorKind::Unexpected,
        "Received response with unexpected status code",
    )
    .with_context("status", response.status().to_string())
    .with_context(
        "headers",
        format_headers_redacted(response.headers(), disable_header_redaction),
    );

    let bytes = match response.bytes().await {
        Ok(bytes) => bytes,
        Err(err) => return err.into(),
    };

    if bytes.is_empty() {
        return err;
    }
    err.with_context("json", String::from_utf8_lossy(&bytes))
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::io;
    use std::sync::Once;

    use mockito::{Matcher, Mock, Server, ServerGuard};
    use tracing_subscriber::fmt::format::FmtSpan;

    use super::*;
    use crate::trace::with_trace_id;

    #[test]
    fn test_cached_token_static_never_expires() {
        let t = CachedToken::from_static("abc".to_string());
        assert!(!t.is_expired());
        assert!(t.expires_at.is_none());
    }

    #[test]
    fn test_cached_token_with_long_expiry_is_not_expired() {
        let t = CachedToken::new("abc".to_string(), Some(3600));
        assert!(!t.is_expired());
    }

    #[test]
    fn test_cached_token_with_short_expiry_is_immediately_expired() {
        // expires_in (1s) is smaller than the 30s safety margin, so the
        // computed lifetime saturates to zero and the token is expired now.
        let t = CachedToken::new("abc".to_string(), Some(1));
        assert!(t.is_expired());
    }

    #[test]
    fn test_cached_token_with_zero_expiry_is_immediately_expired() {
        let t = CachedToken::new("abc".to_string(), Some(0));
        assert!(t.is_expired());
    }

    #[test]
    fn test_cached_token_no_expires_in_never_expires() {
        let t = CachedToken::new("abc".to_string(), None);
        assert!(!t.is_expired());
        assert!(t.expires_at.is_none());
    }

    #[test]
    fn test_format_headers_redacted_empty() {
        let headers = HeaderMap::new();
        let result = format_headers_redacted(&headers, false);
        assert_eq!(result, "{}");
    }

    #[test]
    fn test_format_headers_redacted_non_sensitive() {
        let mut headers = HeaderMap::new();
        headers.insert("content-type", "application/json".parse().unwrap());
        headers.insert("x-request-id", "abc123".parse().unwrap());

        let result = format_headers_redacted(&headers, false);

        assert!(result.contains("content-type"));
        assert!(result.contains("application/json"));
        assert!(result.contains("x-request-id"));
        assert!(result.contains("abc123"));
    }

    #[test]
    fn test_format_headers_redacted_filters_sensitive() {
        let mut headers = HeaderMap::new();
        headers.insert("authorization", "Bearer secret-token".parse().unwrap());
        headers.insert("content-type", "application/json".parse().unwrap());

        let result = format_headers_redacted(&headers, false);

        // Sensitive header should be present but with redacted value
        assert!(result.contains("authorization"));
        assert!(result.contains("[REDACTED]"));
        // Sensitive value should NOT be present
        assert!(!result.contains("secret-token"));
        // Non-sensitive header should be present with actual value
        assert!(result.contains("content-type"));
        assert!(result.contains("application/json"));
    }

    #[test]
    fn test_format_headers_redacted_filters_set_cookie() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "set-cookie",
            "CF_Authorization=sensitive-session-token; Path=/; Secure;"
                .parse()
                .unwrap(),
        );
        headers.insert("server", "cloudflare".parse().unwrap());

        let result = format_headers_redacted(&headers, false);

        // Sensitive header should be present but with redacted value
        assert!(result.contains("set-cookie"));
        assert!(result.contains("[REDACTED]"));
        // Sensitive value should NOT be present
        assert!(!result.contains("sensitive-session-token"));
        // Non-sensitive header should be present with actual value
        assert!(result.contains("server"));
        assert!(result.contains("cloudflare"));
    }

    #[test]
    fn test_format_headers_redacted_filters_all_sensitive() {
        let mut headers = HeaderMap::new();
        headers.insert("authorization", "Bearer token".parse().unwrap());
        headers.insert("proxy-authorization", "Basic creds".parse().unwrap());
        headers.insert("set-cookie", "session=abc".parse().unwrap());
        headers.insert("cookie", "session=abc".parse().unwrap());
        headers.insert("x-api-key", "api-key-123".parse().unwrap());
        headers.insert("x-auth-token", "auth-token-456".parse().unwrap());
        headers.insert("x-request-id", "req-123".parse().unwrap());

        let result = format_headers_redacted(&headers, false);

        // All sensitive headers should be present but with redacted values
        assert!(result.contains("authorization"));
        assert!(result.contains("proxy-authorization"));
        assert!(result.contains("set-cookie"));
        assert!(result.contains("cookie"));
        assert!(result.contains("x-api-key"));
        assert!(result.contains("x-auth-token"));
        assert!(result.contains("[REDACTED]"));

        // Ensure no sensitive values leaked
        assert!(!result.contains("Bearer token"));
        assert!(!result.contains("Basic creds"));
        assert!(!result.contains("session=abc"));
        assert!(!result.contains("api-key-123"));
        assert!(!result.contains("auth-token-456"));

        // Non-sensitive header should be present with actual value
        assert!(result.contains("x-request-id"));
        assert!(result.contains("req-123"));
    }

    #[test]
    fn test_format_headers_with_redaction_disabled() {
        let mut headers = HeaderMap::new();
        headers.insert("authorization", "Bearer secret-token".parse().unwrap());
        headers.insert("x-api-key", "api-key-123".parse().unwrap());
        headers.insert("content-type", "application/json".parse().unwrap());

        let result = format_headers_redacted(&headers, true);

        // When redaction is disabled, all headers and values should be present
        assert!(result.contains("authorization"));
        assert!(result.contains("Bearer secret-token"));
        assert!(result.contains("x-api-key"));
        assert!(result.contains("api-key-123"));
        assert!(result.contains("content-type"));
        assert!(result.contains("application/json"));
        // [REDACTED] should NOT be present when redaction is disabled
        assert!(!result.contains("[REDACTED]"));
    }

    const TRACE_ID: u128 = 0x0af7651916cd43dd8448eb211c80319c;
    const UUID_V4: &str = "^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$";

    thread_local! {
        static LOGS: RefCell<Vec<u8>> = const { RefCell::new(Vec::new()) };
    }

    /// Writes into the current thread's `LOGS`.
    struct ThreadLogs;

    impl io::Write for ThreadLogs {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            LOGS.with_borrow_mut(|logs| logs.extend_from_slice(buf));
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    /// Starts capturing this thread's DEBUG output; read it with `captured_logs`.
    /// The subscriber is global: with `set_default`, another test thread can be the
    /// first to hit the `http` callsite and cache `Interest::never` for everyone. The
    /// writer is per-thread and `#[tokio::test]` is current-thread, so a test sees
    /// only its own spans.
    fn capture_logs() {
        static INIT: Once = Once::new();
        INIT.call_once(|| {
            let subscriber = tracing_subscriber::fmt()
                .with_writer(|| ThreadLogs)
                .with_ansi(false)
                .with_span_events(FmtSpan::CLOSE)
                .with_max_level(tracing::Level::DEBUG)
                .finish();
            tracing::subscriber::set_global_default(subscriber).unwrap();
            // A callsite registered while the global default was being set cached `never`.
            tracing::callsite::rebuild_interest_cache();
        });
        LOGS.with_borrow_mut(Vec::clear);
    }

    fn captured_logs() -> String {
        LOGS.with_borrow(|logs| String::from_utf8(logs.clone()).unwrap())
    }

    /// The close line of every `http` span, in the order the spans closed.
    fn http_spans(logs: &str) -> Vec<&str> {
        logs.lines()
            .filter(|line| {
                line.contains(" http{") && line.contains("iceberg_catalog_rest::client: close")
            })
            .collect()
    }

    /// The value of `name` among a span's fields, as the fmt layer printed it
    /// (`&str` fields are quoted, `%` fields are not).
    fn field<'a>(span: &'a str, name: &str) -> Option<&'a str> {
        let fields = &span[span.find("http{")? + "http{".len()..span.find("}: ")?];
        fields
            .split(' ')
            .find_map(|kv| kv.strip_prefix(name)?.strip_prefix('='))
    }

    fn http_client(uri: String, props: &[(&str, &str)]) -> HttpClient {
        let props = props
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        HttpClient::new(&RestCatalogConfig::builder().uri(uri).props(props).build()).unwrap()
    }

    async fn create_token_mock(server: &mut ServerGuard, expect: usize) -> Mock {
        server
            .mock("POST", "/v1/oauth/tokens")
            .with_status(200)
            .with_body(
                r#"{"access_token":"ey000000000000","token_type":"Bearer","issued_token_type":"urn:ietf:params:oauth:token-type:access_token","expires_in":86400}"#,
            )
            .expect(expect)
            .create_async()
            .await
    }

    /// One `GET /v1/namespaces` under `TRACE_ID`, with a credential. The first try
    /// gets a 401, so the client refreshes its token and retries once. Returns the
    /// captured logs: spans for token, 401, token refresh, retry.
    async fn list_namespaces_with_401_retry() -> String {
        let mut server = Server::new_async().await;
        let token_mock = create_token_mock(&mut server, 2).await;
        let unauthorized = server
            .mock("GET", "/v1/namespaces")
            .with_status(401)
            .expect(1)
            .create_async()
            .await;
        let ok = server
            .mock("GET", "/v1/namespaces")
            .with_status(200)
            .expect(1)
            .create_async()
            .await;
        let client = http_client(server.url(), &[("credential", "client1:secret1")]);
        capture_logs();

        let request = client
            .request(Method::GET, format!("{}/v1/namespaces", server.url()))
            .build()
            .unwrap();
        let response = with_trace_id(TRACE_ID, client.query_catalog(request))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        token_mock.assert_async().await;
        unauthorized.assert_async().await;
        ok.assert_async().await;
        captured_logs()
    }

    #[tokio::test]
    async fn test_request_in_trace_sends_headers_and_records_span() {
        let mut server = Server::new_async().await;
        let mock = server
            .mock("GET", "/v1/config")
            .match_header("x-request-id", Matcher::Regex(UUID_V4.to_string()))
            .match_header(
                "traceparent",
                Matcher::Regex(format!("^00-{TRACE_ID:032x}-[0-9a-f]{{16}}-01$")),
            )
            .with_status(200)
            // Echo the headers so the test can tie them to the span's fields.
            .with_body_from_request(|request| {
                let header = |name: &str| request.header(name)[0].to_str().unwrap().to_owned();
                format!("{} {}", header("x-request-id"), header("traceparent")).into_bytes()
            })
            .create_async()
            .await;
        let client = http_client(server.url(), &[]);
        capture_logs();

        let request = client
            .request(Method::GET, format!("{}/v1/config", server.url()))
            .build()
            .unwrap();
        let response = with_trace_id(TRACE_ID, client.execute(request))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        mock.assert_async().await;
        let echoed = response.text().await.unwrap();
        let (sent_request_id, sent_traceparent) = echoed.split_once(' ').unwrap();
        let logs = captured_logs();
        let spans = http_spans(&logs);
        assert_eq!(spans.len(), 1, "{logs}");
        let span = spans[0];
        assert_eq!(field(span, "http.request.method"), Some("GET"));
        assert_eq!(field(span, "url.path"), Some("\"/v1/config\""));
        assert_eq!(field(span, "server.address"), Some("\"127.0.0.1\""));
        assert_eq!(field(span, "http.response.status_code"), Some("200"));
        assert_eq!(field(span, "polaris.request_id"), Some(sent_request_id));
        assert_eq!(
            field(span, "trace_id"),
            Some(format!("{TRACE_ID:032x}").as_str())
        );
        let span_id = field(span, "span_id").unwrap();
        assert_eq!(sent_traceparent, format!("00-{TRACE_ID:032x}-{span_id}-01"));
        assert!(field(span, "elapsed_ms").is_some());
    }

    #[tokio::test]
    async fn test_request_outside_trace_has_no_traceparent() {
        let mut server = Server::new_async().await;
        let mock = server
            .mock("GET", "/v1/config")
            .match_header("x-request-id", Matcher::Regex(UUID_V4.to_string()))
            .match_header("traceparent", Matcher::Missing)
            .with_status(200)
            .create_async()
            .await;
        let client = http_client(server.url(), &[]);
        capture_logs();

        let request = client
            .request(Method::GET, format!("{}/v1/config", server.url()))
            .build()
            .unwrap();
        client.execute(request).await.unwrap();

        mock.assert_async().await;
        let logs = captured_logs();
        let spans = http_spans(&logs);
        assert_eq!(spans.len(), 1, "{logs}");
        assert_eq!(field(spans[0], "trace_id"), None);
        assert_eq!(field(spans[0], "span_id"), None);
    }

    #[tokio::test]
    async fn test_401_retry_is_its_own_span_in_the_same_trace() {
        let logs = list_namespaces_with_401_retry().await;
        let spans = http_spans(&logs);
        assert_eq!(spans.len(), 4, "{logs}");
        let attempts: Vec<&str> = spans
            .iter()
            .copied()
            .filter(|span| field(span, "url.path") == Some("\"/v1/namespaces\""))
            .collect();
        assert_eq!(attempts.len(), 2, "{logs}");
        assert_eq!(field(attempts[0], "http.response.status_code"), Some("401"));
        assert_eq!(field(attempts[1], "http.response.status_code"), Some("200"));
        assert_ne!(
            field(attempts[0], "polaris.request_id"),
            field(attempts[1], "polaris.request_id")
        );
        assert_ne!(field(attempts[0], "span_id"), field(attempts[1], "span_id"));
        let trace_id = format!("{TRACE_ID:032x}");
        assert!(
            spans
                .iter()
                .all(|span| field(span, "trace_id") == Some(trace_id.as_str()))
        );
    }

    #[tokio::test]
    async fn test_token_exchange_is_traced() {
        let mut server = Server::new_async().await;
        let token_mock = create_token_mock(&mut server, 1).await;
        let config_mock = server
            .mock("GET", "/v1/config")
            .with_status(200)
            .create_async()
            .await;
        let client = http_client(server.url(), &[("credential", "client1:secret1")]);
        capture_logs();

        let request = client
            .request(Method::GET, format!("{}/v1/config", server.url()))
            .build()
            .unwrap();
        client.query_catalog(request).await.unwrap();

        token_mock.assert_async().await;
        config_mock.assert_async().await;
        let logs = captured_logs();
        let spans = http_spans(&logs);
        assert_eq!(spans.len(), 2, "{logs}");
        assert_eq!(field(spans[0], "http.request.method"), Some("POST"));
        assert_eq!(field(spans[0], "url.path"), Some("\"/v1/oauth/tokens\""));
        assert_eq!(field(spans[0], "http.response.status_code"), Some("200"));
    }

    #[tokio::test]
    async fn test_transport_failure_records_error() {
        // A server that accepts one connection and closes it without answering. The
        // port stays bound until then, so no parallel test can take it.
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let uri = format!("http://{}", listener.local_addr().unwrap());
        std::thread::spawn(move || drop(listener.accept()));
        let client = http_client(uri.clone(), &[]);
        capture_logs();

        let request = client
            .request(Method::GET, format!("{uri}/v1/config"))
            .build()
            .unwrap();
        assert!(client.execute(request).await.is_err());

        let logs = captured_logs();
        let spans = http_spans(&logs);
        assert_eq!(spans.len(), 1, "{logs}");
        assert!(field(spans[0], "error").is_some(), "{logs}");
        assert_eq!(field(spans[0], "http.response.status_code"), None);
        assert!(field(spans[0], "elapsed_ms").is_some());
    }

    #[tokio::test]
    async fn test_no_credentials_in_traces() {
        let logs = list_namespaces_with_401_retry().await;
        assert_eq!(http_spans(&logs).len(), 4, "{logs}");
        for leaked in ["secret1", "client_secret", "ey000000000000", "Bearer"] {
            assert!(!logs.contains(leaked), "{leaked} leaked: {logs}");
        }
        assert!(
            !logs.to_ascii_lowercase().contains("authorization"),
            "{logs}"
        );
    }
}
