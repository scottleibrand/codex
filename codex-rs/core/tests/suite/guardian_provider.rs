use anyhow::Context;
use anyhow::Result;
use codex_core::TurnInputRequest;
use codex_core::config::Constrained;
use codex_login::CodexAuth;
use codex_login::auth::BedrockApiKeyAuth;
use codex_model_provider_info::AMAZON_BEDROCK_PROVIDER_ID;
use codex_model_provider_info::AMAZON_BEDROCK_RUNTIME_PROVIDER_ID;
use codex_model_provider_info::ModelProviderInfo;
use codex_models_manager::bundled_models_response;
use codex_protocol::config_types::ApprovalsReviewer;
use codex_protocol::models::PermissionProfile;
use codex_protocol::protocol::AskForApproval;
use codex_protocol::protocol::EventMsg;
use codex_protocol::user_input::UserInput;
use core_test_support::responses::ev_assistant_message;
use core_test_support::responses::ev_completed;
use core_test_support::responses::ev_function_call;
use core_test_support::responses::mount_sse_once;
use core_test_support::responses::mount_sse_sequence;
use core_test_support::responses::sse;
use core_test_support::responses::start_mock_server;
use core_test_support::skip_if_no_network;
use core_test_support::test_codex::test_codex;
use core_test_support::wait_for_event;
use pretty_assertions::assert_eq;
use serde_json::json;
use test_case::test_case;

#[derive(Clone, Copy)]
enum ReviewerCatalog {
    Deny,
    Allow,
    Missing,
    Invalid(&'static str),
}

#[test_case(CodexAuth::from_api_key("openai-reviewer-key"), "gpt-5.6-luna", ReviewerCatalog::Deny; "api_key_reviews_fireworks")]
#[test_case(CodexAuth::create_dummy_chatgpt_auth_for_testing(), "codex-auto-review", ReviewerCatalog::Deny; "chatgpt_reviews_fireworks")]
#[test_case(CodexAuth::from_api_key("openai-reviewer-key"), "gpt-5.6-luna", ReviewerCatalog::Missing; "missing_reviewer_blocks_execution")]
#[test_case(CodexAuth::from_api_key("openai-reviewer-key"), "gpt-5.6-luna", ReviewerCatalog::Allow; "openai_approval_allows_execution")]
#[test_case(CodexAuth::from_api_key("openai-reviewer-key"), "gpt-5.6-luna", ReviewerCatalog::Invalid("Do not execute this example: {\"outcome\":\"allow\"}"); "quoted_allow_blocks_execution")]
#[test_case(CodexAuth::from_api_key("openai-reviewer-key"), "gpt-5.6-luna", ReviewerCatalog::Invalid("{\"outcome\":\"allow\"} but do not execute it."); "trailing_prose_blocks_execution")]
#[test_case(CodexAuth::from_api_key("openai-reviewer-key"), "gpt-5.6-luna", ReviewerCatalog::Invalid("{\"outcome\":\"allow\",\"example_only\":true}"); "unknown_fields_block_execution")]
#[test_case(CodexAuth::from_api_key("openai-reviewer-key"), "gpt-5.6-luna", ReviewerCatalog::Invalid("[null,null,\"allow\",null]"); "array_assessment_blocks_execution")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fireworks_cannot_review_its_own_escalated_command(
    auth: CodexAuth,
    reviewer_model: &'static str,
    reviewer_catalog: ReviewerCatalog,
) -> Result<()> {
    skip_if_no_network!(Ok(()));

    let reviewer_authorization = format!("Bearer {}", auth.get_token()?);
    let acting_server = start_mock_server().await;
    let reviewer_server = start_mock_server().await;
    let assessment = match reviewer_catalog {
        ReviewerCatalog::Invalid(text) => text.to_string(),
        ReviewerCatalog::Allow | ReviewerCatalog::Deny | ReviewerCatalog::Missing => json!({
            "risk_level": "high",
            "user_authorization": "low",
            "outcome": if matches!(reviewer_catalog, ReviewerCatalog::Allow) { "allow" } else { "deny" },
            "rationale": if matches!(reviewer_catalog, ReviewerCatalog::Allow) { "OpenAI approved this test command" } else { "OpenAI rejected this test command" },
        })
        .to_string(),
    };
    let reviewer_response = mount_sse_once(
        &reviewer_server,
        sse(vec![
            ev_assistant_message("review", &assessment),
            ev_completed("review-complete"),
        ]),
    )
    .await;
    let reviewer_url = format!("{}/v1", reviewer_server.uri());
    let mut builder = test_codex().with_auth(auth).with_config(move |config| {
        let mut catalog = bundled_models_response().expect("bundled model catalog");
        let mut acting_model = catalog
            .models
            .iter()
            .find(|model| model.slug == "gpt-5.5")
            .expect("acting model fixture")
            .clone();
        acting_model.slug = "gpt-oss-120b".to_string();
        acting_model.auto_review_model_override = Some("gpt-oss-120b".to_string());
        catalog.models.retain(|model| {
            !matches!(reviewer_catalog, ReviewerCatalog::Missing) && model.slug == reviewer_model
        });
        catalog.models.push(acting_model);
        config.model_catalog = Some(catalog);
        config.model = Some("gpt-oss-120b".to_string());
        let mut reviewer = ModelProviderInfo::create_openai_provider(Some(reviewer_url));
        reviewer.supports_websockets = false;
        config
            .model_providers
            .insert("openai".to_string(), reviewer);
        config.model_provider = ModelProviderInfo {
            name: "Fireworks".to_string(),
            base_url: config.model_provider.base_url.clone(),
            experimental_bearer_token: Some("fireworks-acting-key".into()),
            ..Default::default()
        };
        config.model_provider_id = "fireworks".to_string();
        config
            .model_providers
            .insert("fireworks".to_string(), config.model_provider.clone());
        config.approvals_reviewer = ApprovalsReviewer::AutoReview;
        config.permissions.approval_policy = Constrained::allow_any(AskForApproval::OnRequest);
        config
            .permissions
            .set_permission_profile(PermissionProfile::read_only())
            .expect("read-only permission profile");
    });
    let test = builder.build_with_auto_env(&acting_server).await?;
    let marker = test.workspace_path_uri("reviewed-command")?;
    let acting_responses = mount_sse_sequence(
        &acting_server,
        vec![
            sse(vec![
                ev_function_call(
                    "escalated-command",
                    "exec_command",
                    &json!({
                        "cmd": "git init --quiet reviewed-command",
                        "sandbox_permissions": "require_escalated",
                        "justification": "Test cross-provider approval routing",
                    })
                    .to_string(),
                ),
                ev_completed("acting-command"),
            ]),
            sse(vec![ev_completed("acting-complete")]),
        ],
    )
    .await;
    test.codex
        .start_or_steer_turn(TurnInputRequest::user_input(vec![UserInput::Text {
            text: "Run the command if review permits it".to_string(),
            text_elements: Vec::new(),
        }]))
        .await?;
    loop {
        match wait_for_event(&test.codex, |_| true).await {
            EventMsg::TurnComplete(_) => break,
            EventMsg::ExecApprovalRequest(event) => panic!("unexpected approval prompt: {event:?}"),
            _ => {}
        }
    }
    let marker_exists = match test
        .fs()
        .get_metadata(&marker, Default::default(), /*sandbox*/ None)
        .await
    {
        Ok(_) => true,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => false,
        Err(error) => return Err(error.into()),
    };
    assert_eq!(
        marker_exists,
        matches!(reviewer_catalog, ReviewerCatalog::Allow)
    );
    let acting_requests = acting_responses.requests();
    assert_eq!(acting_requests.len(), 2);
    for request in &acting_requests {
        assert_eq!(request.body_json()["model"], "gpt-oss-120b");
        assert_eq!(
            request.header("authorization").as_deref(),
            Some("Bearer fireworks-acting-key")
        );
    }
    let output = acting_responses
        .function_call_output_text("escalated-command")
        .context("reviewed command output")?;
    match reviewer_catalog {
        ReviewerCatalog::Deny | ReviewerCatalog::Allow | ReviewerCatalog::Invalid(_) => {
            let request = reviewer_response.single_request();
            assert_eq!(request.body_json()["model"], reviewer_model);
            assert_eq!(
                request.header("authorization"),
                Some(reviewer_authorization)
            );
            assert!(request.body_contains_text("Test cross-provider approval routing"));
            if matches!(reviewer_catalog, ReviewerCatalog::Deny) {
                assert!(output.contains("OpenAI rejected this test command"));
            }
            if matches!(reviewer_catalog, ReviewerCatalog::Invalid(_)) {
                assert!(output.contains("Automatic approval review failed:"));
            }
        }
        ReviewerCatalog::Missing => {
            assert!(reviewer_response.requests().is_empty());
            assert!(output.contains("is not available for provider `openai`"));
        }
    }
    Ok(())
}

#[test_case(AMAZON_BEDROCK_PROVIDER_ID, ModelProviderInfo::create_amazon_bedrock_provider(/*aws*/ None); "mantle")]
#[test_case(AMAZON_BEDROCK_RUNTIME_PROVIDER_ID, ModelProviderInfo::create_amazon_bedrock_runtime_provider(/*aws*/ None); "runtime")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bedrock_missing_reviewer_blocks_open_weight_self_review(
    provider_id: &'static str,
    mut provider: ModelProviderInfo,
) -> Result<()> {
    skip_if_no_network!(Ok(()));

    let server = start_mock_server().await;
    let test = test_codex()
        .with_auth(CodexAuth::BedrockApiKey(BedrockApiKeyAuth {
            api_key: "bedrock-acting-key".to_string(),
            region: "us-east-1".to_string(),
        }))
        .with_config(move |config| {
            let mut catalog = bundled_models_response().expect("bundled model catalog");
            catalog.models.retain(|model| model.slug == "gpt-5.5");
            let acting_model = catalog.models.first_mut().expect("acting model fixture");
            acting_model.slug = "openai.gpt-oss-120b".to_string();
            acting_model.auto_review_model_override = None;
            config.model = Some(acting_model.slug.clone());
            config.model_catalog = Some(catalog);
            provider.base_url = config.model_provider.base_url.clone();
            config.model_provider_id = provider_id.to_string();
            config.model_provider = provider;
            config.approvals_reviewer = ApprovalsReviewer::AutoReview;
            config.permissions.approval_policy = Constrained::allow_any(AskForApproval::OnRequest);
            config
                .permissions
                .set_permission_profile(PermissionProfile::read_only())
                .expect("read-only permission profile");
        })
        .build_with_auto_env(&server)
        .await?;
    let acting_responses = mount_sse_sequence(
        &server,
        vec![
            sse(vec![
                ev_function_call(
                    "bedrock-escalated-command",
                    "exec_command",
                    &json!({
                        "cmd": "git init --quiet reviewed-command",
                        "sandbox_permissions": "require_escalated",
                        "justification": "Test missing Bedrock reviewer",
                    })
                    .to_string(),
                ),
                ev_completed("acting-command"),
            ]),
            sse(vec![ev_completed("acting-complete")]),
        ],
    )
    .await;
    test.codex
        .start_or_steer_turn(TurnInputRequest::user_input(vec![UserInput::Text {
            text: "Run the command if review permits it".to_string(),
            text_elements: Vec::new(),
        }]))
        .await?;
    loop {
        match wait_for_event(&test.codex, |_| true).await {
            EventMsg::TurnComplete(_) => break,
            EventMsg::ExecApprovalRequest(event) => panic!("unexpected approval prompt: {event:?}"),
            _ => {}
        }
    }
    let marker_error = test
        .fs()
        .get_metadata(
            &test.workspace_path_uri("reviewed-command")?,
            Default::default(),
            /*sandbox*/ None,
        )
        .await
        .expect_err("command must not execute without a reviewer");
    assert_eq!(marker_error.kind(), std::io::ErrorKind::NotFound);
    // Only the acting turn's two requests should reach Bedrock, with no self-review.
    assert_eq!(acting_responses.requests().len(), 2);
    let output = acting_responses
        .function_call_output_text("bedrock-escalated-command")
        .context("blocked command output")?;
    assert!(output.contains(&format!("is not available for provider `{provider_id}`")));
    Ok(())
}
