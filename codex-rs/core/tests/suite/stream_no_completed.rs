//! Verifies that the agent retries when the SSE stream terminates before
//! delivering a `response.completed` event.

use codex_core::TurnInputRequest;
use codex_core::config::Constrained;
use codex_model_provider_info::ModelProviderInfo;
use codex_model_provider_info::WireApi;
use codex_protocol::protocol::AskForApproval;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::Op;
use codex_protocol::protocol::ReviewDecision;
use codex_protocol::user_input::UserInput;
use core_test_support::responses;
use core_test_support::skip_if_no_network;
use core_test_support::streaming_sse::StreamingSseChunk;
use core_test_support::streaming_sse::start_streaming_sse_server;
use core_test_support::test_codex::TestCodex;
use core_test_support::test_codex::test_codex;
use core_test_support::wait_for_event;
use pretty_assertions::assert_eq;
use std::net::TcpListener;
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener as TokioTcpListener;
use tokio::sync::oneshot;
use wiremock::MockServer;

fn sse_incomplete() -> String {
    responses::sse(vec![serde_json::json!({
        "type": "response.output_item.done",
    })])
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn retries_on_early_close() {
    skip_if_no_network!();

    let incomplete_sse = sse_incomplete();
    let completed_sse = responses::sse_completed("resp_ok");

    let (server, _) = start_streaming_sse_server(vec![
        vec![StreamingSseChunk {
            gate: None,
            body: incomplete_sse,
        }],
        vec![StreamingSseChunk {
            gate: None,
            body: completed_sse,
        }],
    ])
    .await;

    // Configure retry behavior explicitly to avoid mutating process-wide
    // environment variables.

    let model_provider = ModelProviderInfo {
        name: "openai".into(),
        base_url: Some(format!("{}/v1", server.uri())),
        // Environment variable that should exist in the test environment.
        // ModelClient will return an error if the environment variable for the
        // provider is not set.
        env_key: Some("PATH".into()),
        env_key_instructions: None,
        experimental_bearer_token: None,
        auth: None,
        aws: None,
        wire_api: WireApi::Responses,
        query_params: None,
        http_headers: None,
        env_http_headers: None,
        // exercise retry path: first attempt yields incomplete stream, so allow 1 retry
        request_max_retries: Some(0),
        stream_max_retries: Some(1),
        stream_idle_timeout_ms: Some(2000),
        stream_setup_timeout_ms: None,
        sampling_timeout_ms: None,
        websocket_connect_timeout_ms: None,
        requires_openai_auth: false,
        supports_websockets: false,
        supports_standalone_web_search: false,
    };

    let TestCodex { codex, .. } = test_codex()
        .with_config(move |config| {
            config.model_provider = model_provider;
        })
        .build_with_streaming_server(&server)
        .await
        .unwrap();

    codex
        .start_or_steer_turn(TurnInputRequest::user_input(vec![UserInput::Text {
            text: "hello".into(),
            text_elements: Vec::new(),
        }]))
        .await
        .unwrap();

    // Wait until TurnComplete (should succeed after retry).
    wait_for_event(&codex, |event| matches!(event, EventMsg::TurnComplete(_))).await;

    let requests = server.requests().await;
    assert_eq!(
        requests.len(),
        2,
        "expected retry after incomplete SSE stream"
    );

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn retries_when_transport_keepalives_have_no_response_events() {
    skip_if_no_network!();

    let (first_keepalive_tx, first_keepalive_rx) = oneshot::channel();
    let (second_keepalive_tx, second_keepalive_rx) = oneshot::channel();
    let (hold_open_tx, hold_open_rx) = oneshot::channel::<()>();
    let stalled_stream = vec![
        StreamingSseChunk {
            gate: Some(first_keepalive_rx),
            body: ": keepalive\n\n".to_string(),
        },
        StreamingSseChunk {
            gate: Some(second_keepalive_rx),
            body: ": keepalive\n\n".to_string(),
        },
        StreamingSseChunk {
            gate: Some(hold_open_rx),
            body: ": keepalive\n\n".to_string(),
        },
    ];
    let completed_sse = responses::sse_completed("resp_ok");
    let (server, _) = start_streaming_sse_server(vec![
        stalled_stream,
        vec![StreamingSseChunk {
            gate: None,
            body: completed_sse,
        }],
    ])
    .await;

    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(30)).await;
        let _ = first_keepalive_tx.send(());
        tokio::time::sleep(Duration::from_millis(30)).await;
        let _ = second_keepalive_tx.send(());
    });

    let model_provider = ModelProviderInfo {
        name: "openai".into(),
        base_url: Some(format!("{}/v1", server.uri())),
        env_key: Some("PATH".into()),
        env_key_instructions: None,
        experimental_bearer_token: None,
        auth: None,
        aws: None,
        wire_api: WireApi::Responses,
        query_params: None,
        http_headers: None,
        env_http_headers: None,
        request_max_retries: Some(0),
        stream_max_retries: Some(1),
        stream_idle_timeout_ms: Some(2000),
        stream_setup_timeout_ms: None,
        sampling_timeout_ms: None,
        websocket_connect_timeout_ms: None,
        requires_openai_auth: false,
        supports_websockets: false,
        supports_standalone_web_search: false,
    };

    let TestCodex { codex, .. } = test_codex()
        .with_config(move |config| {
            config.model_provider = model_provider;
        })
        .build_with_streaming_server(&server)
        .await
        .unwrap();

    codex
        .start_or_steer_turn(TurnInputRequest::user_input(vec![UserInput::Text {
            text: "hello".into(),
            text_elements: Vec::new(),
        }]))
        .await
        .unwrap();

    wait_for_event(&codex, |event| matches!(event, EventMsg::TurnComplete(_))).await;

    let requests = server.requests().await;
    assert_eq!(
        requests.len(),
        2,
        "expected retry after response-event idle timeout"
    );

    drop(hold_open_tx);
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn retries_when_response_headers_never_arrive() {
    skip_if_no_network!();

    let listener = TokioTcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind response-header stall server");
    let address = listener
        .local_addr()
        .expect("response-header stall server address");
    let completed_sse = responses::sse_completed("resp_ok");
    let (hold_open_tx, hold_open_rx) = oneshot::channel::<()>();
    let (first_request_tx, first_request_rx) = oneshot::channel::<()>();
    let (retry_request_tx, retry_request_rx) = oneshot::channel::<()>();
    let server_task = tokio::spawn(async move {
        let (stalled, _) = listener.accept().await.expect("accept stalled request");
        let _ = first_request_tx.send(());
        tokio::spawn(async move {
            let _stalled = stalled;
            let _ = hold_open_rx.await;
        });

        let (mut retry, _) = listener.accept().await.expect("accept retry request");
        let _ = retry_request_tx.send(());
        let response = format!(
            "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
            completed_sse.len(),
            completed_sse
        );
        retry
            .write_all(response.as_bytes())
            .await
            .expect("write completed SSE response");
        retry.shutdown().await.expect("close retry response");
    });

    let bootstrap_server = responses::start_mock_server().await;
    let base_url = format!("http://{address}/v1");
    let model_provider = ModelProviderInfo {
        name: "openai".into(),
        base_url: Some(base_url),
        env_key: Some("PATH".into()),
        env_key_instructions: None,
        experimental_bearer_token: None,
        auth: None,
        aws: None,
        wire_api: WireApi::Responses,
        query_params: None,
        http_headers: None,
        env_http_headers: None,
        request_max_retries: Some(0),
        stream_max_retries: Some(1),
        stream_idle_timeout_ms: Some(2000),
        stream_setup_timeout_ms: Some(5000),
        sampling_timeout_ms: None,
        websocket_connect_timeout_ms: None,
        requires_openai_auth: false,
        supports_websockets: false,
        supports_standalone_web_search: false,
    };
    let TestCodex { codex, config, .. } = test_codex()
        .with_config(move |config| {
            config.model_provider = model_provider;
        })
        .build(&bootstrap_server)
        .await
        .unwrap();
    assert_eq!(config.model_provider.stream_idle_timeout_ms, Some(2000));

    codex
        .start_or_steer_turn(TurnInputRequest::user_input(vec![UserInput::Text {
            text: "hello".into(),
            text_elements: Vec::new(),
        }]))
        .await
        .unwrap();

    tokio::time::timeout(Duration::from_secs(10), first_request_rx)
        .await
        .expect("initial request was not received")
        .expect("initial request marker dropped");
    tokio::time::timeout(Duration::from_secs(20), retry_request_rx)
        .await
        .expect("pre-stream timeout did not trigger a retry")
        .expect("retry request marker dropped");

    tokio::time::timeout(
        Duration::from_secs(5),
        wait_for_event(&codex, |event| matches!(event, EventMsg::TurnComplete(_))),
    )
    .await
    .expect("retry response did not complete the turn");

    drop(hold_open_tx);
    server_task
        .await
        .expect("response-header stall server task");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sampling_deadline_retries_then_completes() {
    skip_if_no_network!();

    let (hold_open_tx, hold_open_rx) = oneshot::channel::<()>();
    let stalled_stream = vec![
        StreamingSseChunk {
            gate: None,
            body: ": keepalive\n\n".to_string(),
        },
        StreamingSseChunk {
            gate: Some(hold_open_rx),
            body: ": keepalive\n\n".to_string(),
        },
    ];
    let completed_sse = responses::sse_completed("resp_ok");
    let (server, _) = start_streaming_sse_server(vec![
        stalled_stream,
        vec![StreamingSseChunk {
            gate: None,
            body: completed_sse,
        }],
    ])
    .await;

    let model_provider = ModelProviderInfo {
        name: "Amazon Bedrock".into(),
        base_url: Some(format!("{}/v1", server.uri())),
        env_key: Some("PATH".into()),
        env_key_instructions: None,
        experimental_bearer_token: None,
        auth: None,
        aws: None,
        wire_api: WireApi::Responses,
        query_params: None,
        http_headers: None,
        env_http_headers: None,
        request_max_retries: Some(0),
        stream_max_retries: Some(1),
        stream_idle_timeout_ms: Some(2_000),
        stream_setup_timeout_ms: Some(2_000),
        sampling_timeout_ms: Some(150),
        websocket_connect_timeout_ms: None,
        requires_openai_auth: false,
        supports_websockets: false,
        supports_standalone_web_search: false,
    };

    let TestCodex { codex, .. } = test_codex()
        .with_config(move |config| {
            config.model_provider = model_provider;
        })
        .build_with_streaming_server(&server)
        .await
        .unwrap();

    codex
        .start_or_steer_turn(TurnInputRequest::user_input(vec![UserInput::Text {
            text: "hello".into(),
            text_elements: Vec::new(),
        }]))
        .await
        .unwrap();

    let EventMsg::TurnComplete(completed) = tokio::time::timeout(
        Duration::from_secs(5),
        wait_for_event(&codex, |event| matches!(event, EventMsg::TurnComplete(_))),
    )
    .await
    .expect("sampling deadline retry did not complete the turn") else {
        unreachable!("predicate guarantees a turn complete event");
    };
    assert_eq!(completed.error, None);
    assert_eq!(
        server.requests().await.len(),
        2,
        "sampling deadline should retry through the streaming retry policy"
    );

    drop(hold_open_tx);
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sampling_deadline_resets_after_each_provider_event() {
    skip_if_no_network!();

    let (created_tx, created_rx) = oneshot::channel::<()>();
    let (message_tx, message_rx) = oneshot::channel::<()>();
    let (completed_tx, completed_rx) = oneshot::channel::<()>();
    let delayed_stream = vec![
        StreamingSseChunk {
            gate: Some(created_rx),
            body: responses::sse(vec![responses::ev_response_created("resp-progress")]),
        },
        StreamingSseChunk {
            gate: Some(message_rx),
            body: responses::sse(vec![responses::ev_assistant_message(
                "msg-progress",
                "still working",
            )]),
        },
        StreamingSseChunk {
            gate: Some(completed_rx),
            body: responses::sse(vec![responses::ev_completed("resp-progress")]),
        },
    ];
    let (server, _) = start_streaming_sse_server(vec![delayed_stream]).await;
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let _ = created_tx.send(());
        tokio::time::sleep(Duration::from_millis(100)).await;
        let _ = message_tx.send(());
        tokio::time::sleep(Duration::from_millis(100)).await;
        let _ = completed_tx.send(());
    });

    let model_provider = ModelProviderInfo {
        name: "Amazon Bedrock".into(),
        base_url: Some(format!("{}/v1", server.uri())),
        env_key: Some("PATH".into()),
        env_key_instructions: None,
        experimental_bearer_token: None,
        auth: None,
        aws: None,
        wire_api: WireApi::Responses,
        query_params: None,
        http_headers: None,
        env_http_headers: None,
        request_max_retries: Some(0),
        stream_max_retries: Some(0),
        stream_idle_timeout_ms: Some(2_000),
        stream_setup_timeout_ms: Some(2_000),
        sampling_timeout_ms: Some(150),
        websocket_connect_timeout_ms: None,
        requires_openai_auth: false,
        supports_websockets: false,
        supports_standalone_web_search: false,
    };

    let TestCodex { codex, .. } = test_codex()
        .with_config(move |config| {
            config.model_provider = model_provider;
        })
        .build_with_streaming_server(&server)
        .await
        .unwrap();

    codex
        .start_or_steer_turn(TurnInputRequest::user_input(vec![UserInput::Text {
            text: "hello".into(),
            text_elements: Vec::new(),
        }]))
        .await
        .unwrap();

    let EventMsg::TurnComplete(completed) = tokio::time::timeout(
        Duration::from_secs(5),
        wait_for_event(&codex, |event| matches!(event, EventMsg::TurnComplete(_))),
    )
    .await
    .expect("progress events should reset the sampling deadline") else {
        unreachable!("predicate guarantees a turn complete event");
    };
    assert_eq!(completed.error, None);
    assert_eq!(
        server.requests().await.len(),
        1,
        "progress within each sampling window must not retry the model request"
    );

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sampling_deadline_stops_after_stream_retry_limit() {
    skip_if_no_network!();

    let (first_hold_tx, first_hold_rx) = oneshot::channel::<()>();
    let (second_hold_tx, second_hold_rx) = oneshot::channel::<()>();
    let stalled_stream = |hold_open_rx| {
        vec![
            StreamingSseChunk {
                gate: None,
                body: ": keepalive\n\n".to_string(),
            },
            StreamingSseChunk {
                gate: Some(hold_open_rx),
                body: ": keepalive\n\n".to_string(),
            },
        ]
    };
    let (server, _) = start_streaming_sse_server(vec![
        stalled_stream(first_hold_rx),
        stalled_stream(second_hold_rx),
    ])
    .await;

    let model_provider = ModelProviderInfo {
        name: "Amazon Bedrock".into(),
        base_url: Some(format!("{}/v1", server.uri())),
        env_key: Some("PATH".into()),
        env_key_instructions: None,
        experimental_bearer_token: None,
        auth: None,
        aws: None,
        wire_api: WireApi::Responses,
        query_params: None,
        http_headers: None,
        env_http_headers: None,
        request_max_retries: Some(0),
        stream_max_retries: Some(1),
        stream_idle_timeout_ms: Some(2_000),
        stream_setup_timeout_ms: Some(2_000),
        sampling_timeout_ms: Some(150),
        websocket_connect_timeout_ms: None,
        requires_openai_auth: false,
        supports_websockets: false,
        supports_standalone_web_search: false,
    };

    let TestCodex { codex, .. } = test_codex()
        .with_config(move |config| {
            config.model_provider = model_provider;
        })
        .build_with_streaming_server(&server)
        .await
        .unwrap();

    codex
        .start_or_steer_turn(TurnInputRequest::user_input(vec![UserInput::Text {
            text: "hello".into(),
            text_elements: Vec::new(),
        }]))
        .await
        .unwrap();

    let EventMsg::TurnComplete(completed) = tokio::time::timeout(
        Duration::from_secs(5),
        wait_for_event(&codex, |event| matches!(event, EventMsg::TurnComplete(_))),
    )
    .await
    .expect("sampling deadline retries did not stop at the configured limit") else {
        unreachable!("predicate guarantees a turn complete event");
    };
    assert_eq!(
        completed.error.map(|error| error.message),
        Some(
            "stream disconnected before completion: sampling deadline exceeded after 150ms"
                .to_string()
        )
    );
    assert_eq!(
        server.requests().await.len(),
        2,
        "one initial attempt plus one configured stream retry should be made"
    );

    drop(first_hold_tx);
    drop(second_hold_tx);
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sampling_deadline_excludes_approval_wait() {
    skip_if_no_network!();

    let server = responses::start_mock_server().await;
    responses::mount_sse_once(
        &server,
        responses::sse(vec![
            responses::ev_exec_command_call("approval-call", "/bin/echo approved"),
            responses::ev_completed("tool-response"),
        ]),
    )
    .await;
    responses::mount_sse_once(
        &server,
        responses::sse(vec![
            responses::ev_assistant_message("msg-1", "approved"),
            responses::ev_completed("done"),
        ]),
    )
    .await;

    let TestCodex { codex, .. } = test_codex()
        .with_config(|config| {
            config.permissions.approval_policy =
                Constrained::allow_any(AskForApproval::UnlessTrusted);
            config.model_provider.sampling_timeout_ms = Some(50);
            config.model_provider.stream_max_retries = Some(1);
        })
        .build(&server)
        .await
        .unwrap();

    codex
        .start_or_steer_turn(TurnInputRequest::user_input(vec![UserInput::Text {
            text: "run an approved command".into(),
            text_elements: Vec::new(),
        }]))
        .await
        .unwrap();

    let approval_event = wait_for_event(&codex, |event| {
        matches!(event, EventMsg::ExecApprovalRequest(_))
    })
    .await;
    let EventMsg::ExecApprovalRequest(approval) = approval_event else {
        unreachable!("predicate guarantees an approval request");
    };

    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(
        server.received_requests().await.unwrap().len(),
        1,
        "approval wait must not consume the sampling deadline or retry the model request"
    );

    codex
        .submit(Op::ExecApproval {
            id: approval.effective_approval_id(),
            turn_id: None,
            decision: ReviewDecision::Approved,
        })
        .await
        .unwrap();

    let EventMsg::TurnComplete(completed) = tokio::time::timeout(
        Duration::from_secs(5),
        wait_for_event(&codex, |event| matches!(event, EventMsg::TurnComplete(_))),
    )
    .await
    .expect("approved command did not complete the turn") else {
        unreachable!("predicate guarantees a turn complete event");
    };
    assert_eq!(completed.error, None);
    assert_eq!(
        server.received_requests().await.unwrap().len(),
        2,
        "approval should be followed by one normal model continuation"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn connection_failure_pauses_retry_budget_until_provider_is_reachable() -> anyhow::Result<()>
{
    skip_if_no_network!(Ok(()));

    let bootstrap_server = responses::start_mock_server().await;
    let unavailable_listener = TcpListener::bind("127.0.0.1:0")?;
    let unavailable_address = unavailable_listener.local_addr()?;
    drop(unavailable_listener);

    let TestCodex { codex, .. } = test_codex()
        .with_config(move |config| {
            config.model_provider.base_url = Some(format!("http://{unavailable_address}/v1"));
            config.model_provider.request_max_retries = Some(0);
            config.model_provider.stream_max_retries = Some(1);
            config.model_provider.supports_websockets = false;
        })
        .build_with_auto_env(&bootstrap_server)
        .await?;

    codex
        .start_or_steer_turn(TurnInputRequest::user_input(vec![UserInput::Text {
            text: "recover after the network returns".into(),
            text_elements: Vec::new(),
        }]))
        .await?;

    let EventMsg::StreamError(connection_error) =
        wait_for_event(&codex, |event| matches!(event, EventMsg::StreamError(_))).await
    else {
        unreachable!("predicate guarantees a stream error event");
    };
    assert_eq!(
        connection_error.message,
        "Reconnecting... waiting for network"
    );

    let recovered_server = MockServer::builder()
        .listener(TcpListener::bind(unavailable_address)?)
        .start()
        .await;
    let response_mock = responses::mount_sse_sequence(
        &recovered_server,
        vec![sse_incomplete(), responses::sse_completed("resp_recovered")],
    )
    .await;

    let EventMsg::StreamError(stream_error) =
        wait_for_event(&codex, |event| matches!(event, EventMsg::StreamError(_))).await
    else {
        unreachable!("predicate guarantees a stream error event");
    };
    assert_eq!(stream_error.message, "Reconnecting... 1/1");

    let EventMsg::TurnComplete(completed) =
        wait_for_event(&codex, |event| matches!(event, EventMsg::TurnComplete(_))).await
    else {
        unreachable!("predicate guarantees a turn complete event");
    };

    assert_eq!(completed.error, None);
    assert_eq!(response_mock.requests().len(), 2);

    Ok(())
}
