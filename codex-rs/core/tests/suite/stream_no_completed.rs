//! Verifies that the agent retries when the SSE stream terminates before
//! delivering a `response.completed` event.

use codex_core::TurnInputRequest;
use codex_model_provider_info::ModelProviderInfo;
use codex_model_provider_info::WireApi;
use codex_model_provider_info::built_in_model_providers;
use codex_protocol::protocol::EventMsg;
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
use tokio::io::AsyncReadExt;
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
async fn sampling_deadline_retries_then_completes() {
    skip_if_no_network!();

    let (hold_open_tx, hold_open_rx) = oneshot::channel::<()>();
    let (server, _) = start_streaming_sse_server(vec![
        vec![
            StreamingSseChunk {
                gate: None,
                body: responses::sse(vec![responses::ev_response_created("stalled")]),
            },
            StreamingSseChunk {
                gate: Some(hold_open_rx),
                body: responses::sse_completed("stalled"),
            },
        ],
        vec![StreamingSseChunk {
            gate: None,
            body: responses::sse_completed("recovered"),
        }],
    ])
    .await;

    let mut model_provider = built_in_model_providers(/* openai_base_url */ None)["openai"].clone();
    model_provider.base_url = Some(format!("{}/v1", server.uri()));
    model_provider.env_key = Some("PATH".into());
    model_provider.request_max_retries = Some(0);
    model_provider.stream_max_retries = Some(1);
    model_provider.sampling_timeout_ms = Some(50);
    model_provider.supports_websockets = false;

    let TestCodex { codex, .. } = test_codex()
        .with_config(move |config| {
            config.model_provider = model_provider;
        })
        .build_with_streaming_server(&server)
        .await
        .unwrap();

    codex
        .start_or_steer_turn(TurnInputRequest::user_input(vec![UserInput::Text {
            text: "recover after the sampling deadline".into(),
            text_elements: Vec::new(),
        }]))
        .await
        .unwrap();

    let mut effective_settings = Vec::new();
    loop {
        match codex
            .next_event()
            .await
            .expect("event stream should remain open")
            .msg
        {
            EventMsg::SamplingSettingsEffective(event) => effective_settings.push(event),
            EventMsg::TurnComplete(_) => break,
            _ => {}
        }
    }
    assert_eq!(
        server.requests().await.len(),
        2,
        "sampling deadline should retry through the streaming retry policy"
    );
    assert_eq!(effective_settings.len(), 2);
    assert_eq!(effective_settings[0].attempt, 0);
    assert_eq!(effective_settings[1].attempt, 1);
    assert_eq!(
        effective_settings[0].sampling_request_id, effective_settings[1].sampling_request_id,
        "transport retries should retain one captured sampling request identity"
    );
    assert_eq!(effective_settings[0].model, effective_settings[1].model);
    assert_eq!(
        effective_settings[0].reasoning_effort,
        effective_settings[1].reasoning_effort
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
        let (mut stalled, _) = listener.accept().await.expect("accept stalled request");
        let _ = first_request_tx.send(());
        read_complete_http_request(&mut stalled).await;
        tokio::spawn(async move {
            let _stalled = stalled;
            let _ = hold_open_rx.await;
        });

        let (mut retry, _) = listener.accept().await.expect("accept retry request");
        let _ = retry_request_tx.send(());
        read_complete_http_request(&mut retry).await;
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

    let mut model_provider = built_in_model_providers(/* openai_base_url */ None)["openai"].clone();
    let base_url = format!("http://{address}/v1");
    model_provider.base_url = Some(base_url.clone());
    model_provider.env_key = None;
    model_provider.experimental_bearer_token = None;
    model_provider.auth = None;
    model_provider.aws = None;
    model_provider.request_max_retries = Some(0);
    model_provider.stream_max_retries = Some(1);
    model_provider.stream_idle_timeout_ms = Some(2_000);
    model_provider.stream_setup_timeout_ms = Some(1_000);
    model_provider.requires_openai_auth = false;
    model_provider.supports_websockets = false;
    let TestCodex { codex, config, .. } = test_codex()
        .with_config(move |config| {
            config.model_provider = model_provider;
        })
        .build_with_base_url(base_url)
        .await
        .unwrap();
    assert_eq!(config.model_provider.stream_setup_timeout_ms, Some(1_000));

    codex
        .start_or_steer_turn(TurnInputRequest::user_input(vec![UserInput::Text {
            text: "hello".into(),
            text_elements: Vec::new(),
        }]))
        .await
        .unwrap();

    tokio::time::timeout(Duration::from_secs(5), first_request_rx)
        .await
        .expect("initial request was not received")
        .expect("initial request marker dropped");
    tokio::time::timeout(Duration::from_secs(10), retry_request_rx)
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

async fn read_complete_http_request(stream: &mut tokio::net::TcpStream) {
    let mut request = Vec::new();
    let mut scratch = [0_u8; 1024];
    loop {
        let count = stream.read(&mut scratch).await.expect("read HTTP request");
        assert!(count > 0, "request closed before its body was complete");
        request.extend_from_slice(&scratch[..count]);
        let Some(header_end) = request.windows(4).position(|window| window == b"\r\n\r\n") else {
            continue;
        };
        let body_start = header_end + 4;
        let headers = String::from_utf8_lossy(&request[..body_start]);
        let content_length = headers
            .lines()
            .find_map(|line| {
                let (name, value) = line.split_once(':')?;
                name.eq_ignore_ascii_case("content-length")
                    .then(|| value.trim().parse::<usize>().ok())
                    .flatten()
            })
            .unwrap_or(0);
        if request.len() >= body_start + content_length {
            return;
        }
    }
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn retries_when_transport_keepalives_have_no_response_events() {
    skip_if_no_network!();

    let mut keepalive_senders = Vec::new();
    let mut stalled_stream = Vec::new();
    for _ in 0..100 {
        let (sender, receiver) = oneshot::channel();
        keepalive_senders.push(sender);
        stalled_stream.push(StreamingSseChunk {
            gate: Some(receiver),
            body: ": keepalive\n\n".to_string(),
        });
    }
    let completed_sse = responses::sse_completed("resp_ok");
    let (server, _) = start_streaming_sse_server(vec![
        stalled_stream,
        vec![StreamingSseChunk {
            gate: None,
            body: completed_sse,
        }],
    ])
    .await;

    let keepalive_task = tokio::spawn(async move {
        for sender in keepalive_senders {
            tokio::time::sleep(Duration::from_millis(100)).await;
            if sender.send(()).is_err() {
                break;
            }
        }
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
        stream_idle_timeout_ms: Some(500),
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

    tokio::time::timeout(
        Duration::from_secs(5),
        wait_for_event(&codex, |event| matches!(event, EventMsg::TurnComplete(_))),
    )
    .await
    .expect("transport keepalives must not postpone the semantic idle deadline");

    let requests = server.requests().await;
    assert_eq!(
        requests.len(),
        2,
        "expected retry after response-event idle timeout"
    );

    keepalive_task.abort();
    server.shutdown().await;
}

