//! Verifies that the agent retries when the SSE stream terminates before
//! delivering a `response.completed` event.

use codex_core::TurnInputRequest;
use codex_model_provider_info::ModelProviderInfo;
use codex_model_provider_info::WireApi;
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
use wiremock::MockServer;

fn sse_incomplete() -> String {
    responses::sse(vec![serde_json::json!({
        "type": "response.output_item.done",
    })])
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn response_setup_timeout_retries_without_losing_the_turn() -> anyhow::Result<()> {
    skip_if_no_network!(Ok(()));
    let server = MockServer::start().await;
    let attempts = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let observed = attempts.clone();
    wiremock::Mock::given(wiremock::matchers::method("POST"))
        .and(wiremock::matchers::path("/v1/responses"))
        .respond_with(move |_request: &wiremock::Request| {
            let template = wiremock::ResponseTemplate::new(200)
                .insert_header("content-type", "text/event-stream")
                .set_body_string(responses::sse_completed("setup-recovered"));
            if observed.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0 {
                template.set_delay(std::time::Duration::from_secs(2))
            } else {
                template
            }
        })
        .mount(&server)
        .await;
    let test = test_codex().with_config(|config| {
        config.model_provider.request_max_retries = Some(0);
        config.model_provider.stream_max_retries = Some(1);
        config.model_provider.stream_setup_timeout_ms = Some(500);
    }).build(&server).await?;
    test.codex.start_or_steer_turn(TurnInputRequest::user_input(vec![UserInput::Text {
        text: "hello".into(), text_elements: Vec::new(),
    }])).await?;
    let completion = wait_for_event(&test.codex, |event| matches!(event, EventMsg::TurnComplete(_))).await;
    let EventMsg::TurnComplete(completion) = completion else { unreachable!() };
    assert_eq!(completion.error, None);
    assert_eq!(attempts.load(std::sync::atomic::Ordering::SeqCst), 2);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sampling_deadline_retries_an_open_stream_before_its_idle_timeout() -> anyhow::Result<()> {
    skip_if_no_network!(Ok(()));
    let (_hold, gate) = tokio::sync::oneshot::channel();
    let (server, _) = start_streaming_sse_server(vec![
        vec![
            StreamingSseChunk { gate: None, body: responses::sse(vec![responses::ev_response_created("open-stream")]) },
            StreamingSseChunk { gate: Some(gate), body: responses::sse_completed("too-late") },
        ],
        vec![StreamingSseChunk { gate: None, body: responses::sse_completed("sampling-recovered") }],
    ]).await;
    let test = test_codex().with_config(|config| {
        config.model_provider.request_max_retries = Some(0);
        config.model_provider.stream_max_retries = Some(1);
        config.model_provider.stream_idle_timeout_ms = Some(5_000);
        config.model_provider.sampling_timeout_ms = Some(150);
    }).build_with_streaming_server(&server).await?;
    test.codex.start_or_steer_turn(TurnInputRequest::user_input(vec![UserInput::Text {
        text: "hello".into(), text_elements: Vec::new(),
    }])).await?;
    let completion = wait_for_event(&test.codex, |event| matches!(event, EventMsg::TurnComplete(_))).await;
    let EventMsg::TurnComplete(completion) = completion else { unreachable!() };
    assert_eq!(completion.error, None);
    assert_eq!(server.requests().await.len(), 2);
    server.shutdown().await;
    Ok(())
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
        model_catalog_url: None,
        // Environment variable that should exist in the test environment.
        // ModelClient will return an error if the environment variable for the
        // provider is not set.
        env_key: Some("PATH".into()),
        env_key_instructions: None,
        experimental_bearer_token: None,
        auth: None,
        gateway_oauth: None,
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
        capabilities: None,
        include_internal_metadata: false,
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
            config.features.enable(codex_features::Feature::UnboundedConnectionRetries).unwrap();
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
    assert_eq!(stream_error.message, "Reconnecting... 1/1 (waiting 0s)");

    let EventMsg::TurnComplete(completed) =
        wait_for_event(&codex, |event| matches!(event, EventMsg::TurnComplete(_))).await
    else {
        unreachable!("predicate guarantees a turn complete event");
    };

    assert_eq!(completed.error, None);
    assert_eq!(response_mock.requests().len(), 2);

    Ok(())
}
