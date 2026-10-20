use codex_utils_absolute_path::test_support::PathExt;
use pretty_assertions::assert_eq;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use uuid::Uuid;

use super::*;

#[test]
fn default_filter_caps_high_volume_targets_and_keeps_warnings() {
    use tracing::Level;

    let filter = filter_with_overrides(None);
    let cases = [
        ("codex_core::exec_policy", Level::DEBUG),
        ("codex_core::exec_policy::executable_identity", Level::DEBUG),
        ("codex_app_server::outgoing_message", Level::DEBUG),
        ("codex_core::stream_events_utils", Level::INFO),
        ("codex_core::session::handlers", Level::INFO),
        ("codex_http_client::client", Level::INFO),
    ];
    for (target, most_verbose) in cases {
        for level in [
            Level::ERROR,
            Level::WARN,
            Level::INFO,
            Level::DEBUG,
            Level::TRACE,
        ] {
            assert_eq!(
                filter.would_enable(target, &level),
                level <= most_verbose,
                "{target} at {level}"
            );
        }
    }
    // Neighbouring targets keep their previous levels.
    assert!(filter.would_enable("codex_core::session::turn", &Level::TRACE));
    assert!(filter.would_enable("codex_core::client", &Level::TRACE));
    assert!(filter.would_enable("codex_app_server::message_processor", &Level::TRACE));
    assert!(filter.would_enable("codex_http_client::transport", &Level::DEBUG));
    assert!(!filter.would_enable("codex_http_client::transport", &Level::TRACE));
}

#[test]
fn log_db_filter_overrides_replace_builtin_levels() {
    use tracing::Level;

    let filter = filter_with_overrides(Some(
        "codex_core::exec_policy=trace,codex_core::session::turn=warn",
    ));
    assert!(filter.would_enable("codex_core::exec_policy", &Level::TRACE));
    assert!(!filter.would_enable("codex_core::session::turn", &Level::INFO));
    assert!(filter.would_enable("codex_core::session::turn", &Level::WARN));
    assert!(!filter.would_enable("codex_http_client::client", &Level::DEBUG));

    let filter = filter_with_overrides(Some("info"));
    assert!(!filter.would_enable("codex_state", &Level::DEBUG));
    assert!(filter.would_enable("codex_state", &Level::INFO));
    assert!(!filter.would_enable("sqlx::query", &Level::ERROR));

    for invalid in ["", "   ", "codex_core=verbose"] {
        let filter = filter_with_overrides(Some(invalid));
        assert!(
            filter.would_enable("codex_state", &Level::TRACE),
            "{invalid:?}"
        );
        assert!(!filter.would_enable("codex_core::exec_policy", &Level::TRACE));
    }
}

#[tokio::test]
async fn sqlite_sink_filters_noisy_targets_without_dropping_useful_diagnostics() {
    let codex_home =
        std::env::temp_dir().join(format!("codex-state-log-db-filter-{}", Uuid::new_v4()));
    let _cleanup = scopeguard::guard(codex_home.clone(), |codex_home| {
        let _ = std::fs::remove_dir_all(codex_home);
    });
    let runtime = StateRuntime::init(
        crate::SqliteConfig::new_for_testing(codex_home.as_path().abs()),
        "test-provider".to_string(),
    )
    .await
    .expect("initialize runtime");
    let layer = start(
        runtime.clone(),
        Arc::new(super::tests::SharedWriter::default()),
    );

    let guard = tracing_subscriber::registry()
        .with(layer.clone().with_filter(default_filter()))
        .set_default();

    tracing::trace!(target: "opentelemetry_sdk", "dropped-trace");
    tracing::debug!(target: "opentelemetry_sdk", "dropped-debug");
    tracing::info!(target: "opentelemetry_sdk", "retained-info");
    tracing::warn!(target: "sqlx::query", "dropped-slow-query-warning");
    tracing::warn!(target: "sqlx::pool::acquire", "dropped-slow-acquire-warning");
    tracing::warn!(target: "sqlx::other", "dropped-sqlx-warning");
    tracing::info!(target: "sqlx_application", "retained-unrelated-target");
    tracing::debug!(target: "opentelemetry-otlp", "dropped-otlp-export");
    tracing::error!(target: "opentelemetry-http", "dropped-http-export");
    tracing::trace!(target: "h2::proto::streams", "dropped-http2-trace");
    tracing::debug!(target: "tonic::transport::channel", "dropped-grpc-transport-debug");
    tracing::debug!(target: "tower::buffer::worker", "dropped-grpc-buffer-debug");
    tracing::warn!(
        target: "h2::proto::ping_pong",
        "recv PING ack that we never sent: {:?}",
        "synthetic-ack"
    );
    tracing::warn!(target: "h2::proto::ping_pong", "retained-http2-ping-warning");
    tracing::warn!(target: "h2::proto::streams", "retained-http2-warning");
    tracing::debug!(target: "rmcp::transport", "dropped-rmcp-debug");
    tracing::info!(target: "rmcp::transport", "retained-rmcp-info");
    tracing::trace!(target: "tokio_graceful::guard", "dropped-guard-trace");
    tracing::debug!(target: "tokio_graceful::guard", "retained-guard-debug");
    tracing::trace!(target: "tokio_graceful::trigger", "dropped-trigger-trace");
    tracing::debug!(target: "tokio_graceful::trigger", "retained-trigger-debug");
    tracing::trace!(target: "tokio_graceful::shutdown", "retained-shutdown-trace");
    tracing::debug!(
        target: "codex_rmcp_client::oauth",
        "dropped-codex-rmcp-client-debug"
    );
    tracing::info!(
        target: "codex_rmcp_client::oauth",
        "retained-codex-rmcp-client-info"
    );
    tracing::trace!(target: "codex_http_client::transport", "dropped-request-body");
    tracing::debug!(target: "codex_http_client::transport", "retained-request-diagnostic");
    tracing::trace!(target: "codex_api::sse", "dropped-sse-parent");
    tracing::trace!(target: "codex_api::sse::responses", "dropped-sse-payload");
    tracing::debug!(target: "codex_api::sse::responses", "retained-sse-diagnostic");
    tracing::trace!(target: "codex_state", "retained-trace");
    tracing::trace!(
        target: "codex_tui::streaming::controller",
        "dropped-controller-trace"
    );
    tracing::debug!(
        target: "codex_tui::streaming::controller",
        "retained-controller-debug"
    );
    tracing::trace!(
        target: "codex_tui::streaming::table_holdback",
        "dropped-table-holdback-trace"
    );
    tracing::debug!(
        target: "codex_tui::streaming::table_holdback",
        "retained-table-holdback-debug"
    );
    tracing::trace!(
        target: "codex_tui::streaming::commit_tick",
        "retained-commit-tick-trace"
    );
    tracing::trace!(
        target: "codex_api::responses_websocket_timing",
        payload = "complete timing payload",
        "dropped-websocket-timing"
    );
    tracing::trace!(target: "codex_core::exec_policy", "dropped-exec-policy-trace");
    tracing::debug!(target: "codex_core::exec_policy", "retained-exec-policy-debug");
    tracing::debug!(target: "codex_http_client::client", "dropped-http-client-debug");
    tracing::warn!(target: "codex_http_client::client", "retained-http-client-warning");

    layer.flush().await;
    drop(guard);

    let logs = runtime
        .query_logs(&crate::LogQuery::default())
        .await
        .expect("query logs after flush");
    assert_eq!(
        logs.iter()
            .map(|row| (
                row.level.as_str(),
                row.target.as_str(),
                row.message.as_deref()
            ))
            .collect::<Vec<_>>(),
        vec![
            ("INFO", "opentelemetry_sdk", Some("retained-info")),
            (
                "INFO",
                "sqlx_application",
                Some("retained-unrelated-target")
            ),
            (
                "WARN",
                "h2::proto::ping_pong",
                Some("retained-http2-ping-warning")
            ),
            ("WARN", "h2::proto::streams", Some("retained-http2-warning")),
            ("INFO", "rmcp::transport", Some("retained-rmcp-info")),
            (
                "DEBUG",
                "tokio_graceful::guard",
                Some("retained-guard-debug")
            ),
            (
                "DEBUG",
                "tokio_graceful::trigger",
                Some("retained-trigger-debug")
            ),
            (
                "TRACE",
                "tokio_graceful::shutdown",
                Some("retained-shutdown-trace")
            ),
            (
                "INFO",
                "codex_rmcp_client::oauth",
                Some("retained-codex-rmcp-client-info")
            ),
            (
                "DEBUG",
                "codex_http_client::transport",
                Some("retained-request-diagnostic")
            ),
            (
                "DEBUG",
                "codex_api::sse::responses",
                Some("retained-sse-diagnostic")
            ),
            ("TRACE", "codex_state", Some("retained-trace")),
            (
                "DEBUG",
                "codex_tui::streaming::controller",
                Some("retained-controller-debug"),
            ),
            (
                "DEBUG",
                "codex_tui::streaming::table_holdback",
                Some("retained-table-holdback-debug"),
            ),
            (
                "TRACE",
                "codex_tui::streaming::commit_tick",
                Some("retained-commit-tick-trace"),
            ),
            (
                "DEBUG",
                "codex_core::exec_policy",
                Some("retained-exec-policy-debug"),
            ),
            (
                "WARN",
                "codex_http_client::client",
                Some("retained-http-client-warning"),
            ),
        ]
    );
}
