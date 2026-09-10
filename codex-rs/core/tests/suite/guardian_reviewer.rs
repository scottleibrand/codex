use std::sync::Arc;

use anyhow::Result;
use codex_config::config_toml::AutoReviewToml;
use codex_core::config::Config;
use codex_core::config::GuardianReviewerConfig;
use codex_core::config::resolve_guardian_reviewer;
use codex_login::AuthManager;
use codex_login::CodexAuth;
use codex_models_manager::bundled_models_response;
use codex_models_manager::manager::StaticModelsManager;
use codex_protocol::openai_models::ModelsResponse;
use codex_protocol::openai_models::ReasoningEffort;
use core_test_support::responses;
use core_test_support::test_codex::TestCodex;
use core_test_support::test_codex::test_codex;
use pretty_assertions::assert_eq;

async fn resolve_reviewer(test: &TestCodex, config: &Config) -> Result<GuardianReviewerConfig> {
    let models_manager = test.thread_manager.get_models_manager();
    let acting_model = models_manager
        .get_model_info("gpt-5.5", &config.to_models_manager_config())
        .await;
    resolve_guardian_reviewer(
        config,
        models_manager.as_ref(),
        Some(test.thread_manager.auth_manager()),
        &acting_model,
        /*acting_reasoning_effort*/ None,
    )
    .await
}

#[tokio::test]
async fn owner_configured_reviewer_model_overrides_acting_model_metadata() -> Result<()> {
    let server = responses::start_mock_server().await;
    let test = test_codex()
        .with_auth(CodexAuth::create_dummy_chatgpt_auth_for_testing())
        .with_model_info_override("gpt-5.5", |model| {
            model.auto_review_model_override = Some("gpt-5.2".to_string());
        })
        .build_with_auto_env(&server)
        .await?;

    let mut parent_config = test.config.clone();
    parent_config.auto_review = Some(AutoReviewToml {
        model_provider: Some("openai".to_string()),
        model: Some("codex-auto-review".to_string()),
        reasoning_effort: Some(ReasoningEffort::Low),
        policy: None,
    });

    let options = resolve_reviewer(&test, &parent_config).await?;

    assert_eq!(Some(options.model), Some("codex-auto-review".to_string()));
    Ok(())
}

#[tokio::test]
async fn catalog_reviewer_override_is_used_for_trusted_parent() -> Result<()> {
    let server = responses::start_mock_server().await;
    let test = test_codex()
        .with_auth(CodexAuth::create_dummy_chatgpt_auth_for_testing())
        .with_model_info_override("gpt-5.5", |model| {
            model.auto_review_model_override = Some("gpt-5.2".to_string());
        })
        .with_model("gpt-5.5")
        .build_with_auto_env(&server)
        .await?;

    let options = resolve_reviewer(&test, &test.config).await?;

    assert_eq!(Some(options.model), Some("gpt-5.2".to_string()));
    Ok(())
}

#[tokio::test]
async fn missing_catalog_reviewer_preserves_reasoning_fallback() -> Result<()> {
    let server = responses::start_mock_server().await;
    let test = test_codex()
        .with_auth(CodexAuth::create_dummy_chatgpt_auth_for_testing())
        .with_model_info_override("gpt-5.5", |model| {
            model.auto_review_model_override = Some("uncatalogued-reviewer".to_string());
            model.supported_reasoning_levels.clear();
            model.default_reasoning_level = Some(ReasoningEffort::Medium);
        })
        .with_model("gpt-5.5")
        .build_with_auto_env(&server)
        .await?;
    let options = resolve_reviewer(&test, &test.config).await?;
    assert_eq!(
        (Some(options.model), options.reasoning_effort),
        (
            Some("uncatalogued-reviewer".to_string()),
            Some(ReasoningEffort::Medium)
        ),
    );
    Ok(())
}

#[tokio::test]
async fn missing_owner_reviewer_override_is_not_exempted_by_catalog_override() -> Result<()> {
    let server = responses::start_mock_server().await;
    let test = test_codex()
        .with_model_info_override("gpt-5.5", |model| {
            model.auto_review_model_override = Some("gpt-5.2".to_string());
        })
        .with_config(|config| {
            config.auto_review = Some(AutoReviewToml {
                model: Some("missing-owner-reviewer".to_string()),
                ..Default::default()
            });
        })
        .build_with_auto_env(&server)
        .await?;

    let error = resolve_reviewer(&test, &test.config)
        .await
        .err()
        .expect("missing owner model must fail during reviewer resolution");
    assert!(error.to_string().contains(
        "Guardian reviewer model `missing-owner-reviewer` is not available for provider `openai`"
    ));
    Ok(())
}

#[tokio::test]
async fn merged_config_reviewer_uses_reserved_provider_for_untrusted_parent() -> Result<()> {
    let server = responses::start_mock_server().await;
    let test = test_codex()
        .with_auth(CodexAuth::create_dummy_chatgpt_auth_for_testing())
        .with_model("gpt-5.5")
        .build_with_auto_env(&server)
        .await?;
    let mut parent_config = test.config.clone();
    parent_config.model_provider_id = "fireworks".to_string();
    parent_config.model_provider = codex_model_provider_info::ModelProviderInfo {
        name: "Fireworks".to_string(),
        base_url: Some("https://api.fireworks.ai/inference/v1".to_string()),
        env_key: Some("FIREWORKS_API_KEY".to_string()),
        ..Default::default()
    };
    parent_config.model_providers.insert(
        "fireworks".to_string(),
        parent_config.model_provider.clone(),
    );
    parent_config.auto_review = Some(AutoReviewToml {
        model_provider: Some("openai".to_string()),
        model: Some("codex-auto-review".to_string()),
        reasoning_effort: Some(ReasoningEffort::Low),
        policy: None,
    });

    let options = resolve_reviewer(&test, &parent_config).await?;

    assert_eq!(options.provider_id, "openai");
    let expected_provider = test
        .config
        .model_providers
        .get("openai")
        .expect("built-in OpenAI provider")
        .clone();
    assert_eq!(options.provider, expected_provider);
    assert_eq!(Some(options.model), Some("codex-auto-review".to_string()));
    assert_eq!(options.reasoning_effort, Some(ReasoningEffort::Low));
    Ok(())
}

#[tokio::test]
async fn policy_only_config_preserves_parent_model_fallback() -> Result<()> {
    let server = responses::start_mock_server().await;
    let mut parent_model = bundled_models_response()?
        .models
        .into_iter()
        .find(|model| model.slug == "gpt-5.5")
        .expect("bundled parent model should exist");
    parent_model
        .supported_reasoning_levels
        .retain(|effort| effort.effort != ReasoningEffort::Low);
    parent_model.default_reasoning_level = Some(ReasoningEffort::Medium);
    let auth = CodexAuth::create_dummy_chatgpt_auth_for_testing();
    let auth_manager = AuthManager::from_auth_for_testing(auth.clone());
    let models_manager = StaticModelsManager::new(
        Some(auth_manager),
        ModelsResponse {
            models: vec![parent_model],
        },
    );
    let test = test_codex()
        .with_auth(auth)
        .with_models_manager(Arc::new(models_manager))
        .with_model("gpt-5.5")
        .build_with_auto_env(&server)
        .await?;
    let mut parent_config = test.config.clone();
    parent_config.auto_review = Some(AutoReviewToml {
        model_provider: None,
        model: None,
        reasoning_effort: None,
        policy: Some("Keep the local policy addition.".to_string()),
    });

    let options = resolve_reviewer(&test, &parent_config).await?;

    assert_eq!(Some(options.model), Some("gpt-5.5".to_string()));
    assert_eq!(options.reasoning_effort, Some(ReasoningEffort::Medium));
    Ok(())
}
