use super::ResponsesStreamRequest;
use super::is_unbounded_interactive_retry_error;
use super::log_retry;
use super::response_retry_delay;
use super::setup_retry_backoff;
use super::unbounded_retry_status;
use crate::session::tests::make_session_and_context;
use codex_model_provider_info::built_in_model_providers;
use codex_protocol::error::CodexErr;
use std::time::Duration;
use tracing_test::internal::MockWriter;

#[test]
fn response_setup_timeout_uses_bounded_retries() {
    let err = CodexErr::ResponseStreamSetupTimeout(Duration::from_secs(60));

    assert!(!is_unbounded_interactive_retry_error(&err, true, false));
    assert!(!is_unbounded_interactive_retry_error(&err, false, false));
    assert!(!is_unbounded_interactive_retry_error(&err, true, true));
    assert!(!is_unbounded_interactive_retry_error(&err, false, true));
    assert!(err.is_retryable());
    assert_eq!(
        err.to_string(),
        "stream disconnected before completion: timeout establishing response stream after 60s"
    );
}

#[test]
fn high_demand_uses_unbounded_interactive_retries_on_bedrock() {
    let err = CodexErr::InternalServerError;

    assert!(!is_unbounded_interactive_retry_error(&err, true, false));
    assert!(!is_unbounded_interactive_retry_error(&err, false, false));
    assert!(is_unbounded_interactive_retry_error(&err, true, true));
    assert!(is_unbounded_interactive_retry_error(&err, false, true));
    assert!(err.is_retryable());
    assert_eq!(
        err.to_string(),
        "We’re currently experiencing high demand, which may cause temporary errors."
    );
}

#[test]
fn unbounded_retry_status_includes_retry_counter_and_delay() {
    assert_eq!(
        unbounded_retry_status(3, Duration::from_secs(20)),
        "Reconnecting... retry 3 (waiting 20s)"
    );
}

#[test]
fn setup_timeout_backoff_spans_multi_minute_failures() {
    assert!(setup_retry_backoff(1) < Duration::from_secs(1));
    assert_eq!(setup_retry_backoff(2), Duration::from_secs(60));
    assert_eq!(setup_retry_backoff(3), Duration::from_secs(120));
    assert_eq!(setup_retry_backoff(4), Duration::from_secs(240));
    assert_eq!(setup_retry_backoff(5), Duration::from_secs(480));
    assert_eq!(setup_retry_backoff(6), Duration::from_secs(960));
    assert_eq!(setup_retry_backoff(7), Duration::from_secs(1920));
    assert_eq!(setup_retry_backoff(8), Duration::from_secs(3840));
}

#[test]
fn http_transport_server_errors_use_the_longer_recovery_window() {
    let err = CodexErr::Stream(
        "The server had an error while processing your request. Sorry about that!".into(),
    );
    assert!(err.is_retryable());
    // HTTP transport (Bedrock always, other providers after fallback) waits out outages.
    assert_eq!(
        response_retry_delay(&err, /*websocket_active*/ false, 5),
        Duration::from_secs(480),
    );
    // An active WebSocket retries quickly so the HTTPS fallback is reached promptly.
    assert!(response_retry_delay(&err, /*websocket_active*/ true, 1) < Duration::from_secs(1));
    // Setup timeouts take the long schedule even on an active WebSocket.
    let setup_timeout = CodexErr::ResponseStreamSetupTimeout(Duration::from_secs(60));
    assert_eq!(
        response_retry_delay(&setup_timeout, /*websocket_active*/ true, 2),
        Duration::from_secs(60),
    );
}

#[test]
fn default_retry_budget_outlasts_multi_hour_provider_outages() {
    let openai_base_url = None;
    let providers = built_in_model_providers(openai_base_url);
    let err = CodexErr::Stream("stream closed before response.completed".into());
    for provider in ["amazon-bedrock", "openai"] {
        let max_retries = providers[provider].stream_max_retries();
        let total: Duration = (1..=max_retries)
            .map(|attempt| response_retry_delay(&err, /*websocket_active*/ false, attempt))
            .sum();
        // 2026-09-30: one Bedrock session failed every request for 73 minutes.
        assert!(total >= Duration::from_secs(8 * 3600), "{provider}: {total:?}");
    }
}

#[tokio::test]
async fn sampling_retry_logs_stream_error_context() {
    let (_session, turn_context) = make_session_and_context().await;
    let buffer: &'static std::sync::Mutex<Vec<u8>> =
        Box::leak(Box::new(std::sync::Mutex::new(Vec::new())));
    let subscriber = tracing_subscriber::fmt()
        .with_ansi(false)
        .with_max_level(tracing::Level::WARN)
        .with_writer(MockWriter::new(buffer))
        .finish();
    let _subscriber_guard = tracing::subscriber::set_default(subscriber);

    log_retry(
        ResponsesStreamRequest::Sampling,
        &turn_context,
        &CodexErr::Stream("websocket closed by server before response.completed".to_string()),
        /*retries*/ 2,
        /*max_retries*/ 5,
        Duration::from_secs(1),
    );

    let logs = String::from_utf8(
        buffer
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone(),
    )
    .expect("retry log should be valid utf-8");
    assert!(logs.contains("stream disconnected - retrying sampling request"));
    assert!(logs.contains(&format!("turn_id={}", turn_context.sub_id)));
    assert!(logs.contains("retries=2"));
    assert!(logs.contains("max_retries=5"));
    assert!(logs.contains(
        "sampling_error=stream disconnected before completion: websocket closed by server before response.completed"
    ));
}
