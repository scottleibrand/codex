use std::sync::Arc;

use anyhow::anyhow;
use codex_login::AuthManager;
use codex_model_provider::create_model_provider;
use codex_model_provider_info::AMAZON_BEDROCK_PROVIDER_ID;
use codex_model_provider_info::AMAZON_BEDROCK_RUNTIME_PROVIDER_ID;
use codex_model_provider_info::ModelProviderInfo;
use codex_model_provider_info::OPENAI_PROVIDER_ID;
use codex_models_manager::manager::ModelsManager;
use codex_models_manager::manager::RefreshStrategy;
use codex_protocol::openai_models::ModelInfo;
use codex_protocol::openai_models::ReasoningEffort;

use super::Config;

/// The provider and model selected for one Guardian review session.
#[derive(Debug, Clone)]
pub struct GuardianReviewerConfig {
    /// Reserved provider ID used by the reviewer session.
    pub provider_id: String,
    /// Provider metadata used to construct the reviewer session.
    pub provider: ModelProviderInfo,
    /// Model slug used by the reviewer session.
    pub model: String,
    /// Resolved metadata for the reviewer model.
    pub model_info: ModelInfo,
    /// Reasoning effort used by the reviewer session.
    pub reasoning_effort: Option<ReasoningEffort>,
    /// Default model advertised by the selected reviewer provider.
    pub default_model_id: String,
    /// Whether the default model was present in the offline catalog.
    pub catalog_contains_default: bool,
    /// Whether the owner explicitly configured a reviewer model.
    pub model_overridden: bool,
    /// Owner-configured reviewer model, when present.
    pub model_override: Option<String>,
}

/// Resolves the trusted Guardian reviewer without accepting an untrusted acting
/// model's reviewer override.
///
/// An untrusted acting provider uses the reserved OpenAI provider by default. A
/// reviewer model must be present in the offline catalog; absence is an error so
/// callers fail closed instead of silently reviewing through the acting provider.
/// Trusted OpenAI sessions retain their legacy same-model fallback when no
/// owner-authored reviewer override is present.
/// The host/owner-supplied catalog and configuration are trusted; a separate
/// catalog is not required when the acting provider differs from the reviewer.
pub async fn resolve_guardian_reviewer(
    config: &Config,
    models_manager: &dyn ModelsManager,
    auth_manager: Option<Arc<AuthManager>>,
    acting_model_info: &ModelInfo,
    acting_reasoning_effort: Option<ReasoningEffort>,
) -> anyhow::Result<GuardianReviewerConfig> {
    let configured_reviewer = config.auto_review.as_ref();
    let provider_id = configured_reviewer
        .and_then(|reviewer| reviewer.model_provider.as_deref())
        .map(str::to_owned)
        .unwrap_or_else(|| {
            if is_trusted_reviewer_provider(&config.model_provider_id, &config.model_provider) {
                config.model_provider_id.clone()
            } else {
                OPENAI_PROVIDER_ID.to_string()
            }
        });
    let provider = if provider_id == config.model_provider_id {
        config.model_provider.clone()
    } else {
        config
            .model_providers
            .get(&provider_id)
            .cloned()
            .ok_or_else(|| {
                anyhow!("Guardian reviewer provider `{provider_id}` is not configured")
            })?
    };
    if !is_trusted_reviewer_provider(&provider_id, &provider) {
        return Err(anyhow!(
            "Guardian reviewer provider `{provider_id}` is not trusted"
        ));
    }
    let provider_runtime = create_model_provider(provider.clone(), auth_manager);
    let default_model_id = provider_runtime
        .approval_review_preferred_model()
        .to_string();
    let configured_model = configured_reviewer.and_then(|reviewer| reviewer.model.as_deref());
    let catalog_override = (provider_id == config.model_provider_id
        && is_trusted_reviewer_provider(&config.model_provider_id, &config.model_provider))
    .then_some(acting_model_info.auto_review_model_override.as_deref())
    .flatten();
    let model = configured_model
        .or(catalog_override)
        .unwrap_or(&default_model_id)
        .to_string();
    let available_models = models_manager
        .list_models(RefreshStrategy::Offline, config.http_client_factory())
        .await;
    let catalog_contains_default = available_models
        .iter()
        .any(|preset| preset.model == default_model_id);
    if (configured_model.is_some() || catalog_override.is_none())
        && !available_models.iter().any(|preset| preset.model == model)
    {
        let reviewer_target_overridden = configured_reviewer
            .is_some_and(|reviewer| reviewer.model_provider.is_some() || reviewer.model.is_some());
        // Bedrock also hosts open-weight models, so its provider identity alone
        // cannot authorize falling back to the acting model for review.
        if !reviewer_target_overridden
            && config.model_provider_id == OPENAI_PROVIDER_ID
            && is_trusted_reviewer_provider(&config.model_provider_id, &config.model_provider)
        {
            let fallback_model = acting_model_info.slug.clone();
            let fallback_reasoning_effort = configured_reviewer
                .and_then(|reviewer| reviewer.reasoning_effort.clone())
                .or_else(|| {
                    acting_model_info
                        .supported_reasoning_levels
                        .iter()
                        .find(|effort| effort.effort == ReasoningEffort::Low)
                        .map(|_| ReasoningEffort::Low)
                })
                .or(acting_reasoning_effort)
                .or_else(|| acting_model_info.default_reasoning_level.clone());
            return Ok(GuardianReviewerConfig {
                provider_id: config.model_provider_id.clone(),
                provider: provider.clone(),
                model: fallback_model,
                model_info: acting_model_info.clone(),
                reasoning_effort: fallback_reasoning_effort,
                default_model_id,
                catalog_contains_default,
                model_overridden: false,
                model_override: None,
            });
        }
        return Err(anyhow!(
            "Guardian reviewer model `{model}` is not available for provider `{provider_id}`"
        ));
    }

    let model_info = models_manager
        .get_model_info(&model, &config.to_models_manager_config())
        .await;
    let reasoning_effort = configured_reviewer
        .and_then(|reviewer| reviewer.reasoning_effort.clone())
        .or_else(|| {
            available_models
                .iter()
                .find(|preset| preset.model == model)
                .and_then(|preset| {
                    preset
                        .supported_reasoning_efforts
                        .iter()
                        .find(|effort| effort.effort == ReasoningEffort::Low)
                        .map(|_| ReasoningEffort::Low)
                        .or_else(|| Some(preset.default_reasoning_effort.clone()))
                })
                // Trusted catalog overrides may be absent from the offline
                // catalog; retain the legacy acting-model effort in that case.
                .or_else(|| {
                    acting_model_info
                        .supported_reasoning_levels
                        .iter()
                        .find(|effort| effort.effort == ReasoningEffort::Low)
                        .map(|_| ReasoningEffort::Low)
                        .or(acting_reasoning_effort)
                        .or_else(|| acting_model_info.default_reasoning_level.clone())
                })
        });

    Ok(GuardianReviewerConfig {
        provider_id,
        provider,
        model,
        model_info,
        reasoning_effort,
        default_model_id,
        catalog_contains_default,
        model_overridden: configured_model.is_some() || catalog_override.is_some(),
        model_override: configured_model.or(catalog_override).map(str::to_string),
    })
}

/// Returns whether the provider identity is eligible to review untrusted models.
pub fn is_trusted_reviewer_provider(provider_id: &str, provider: &ModelProviderInfo) -> bool {
    let trusted_openai = provider_id == OPENAI_PROVIDER_ID
        && provider.is_openai()
        && provider.requires_openai_auth
        && provider.env_key.is_none()
        && provider.experimental_bearer_token.is_none()
        && provider.auth.is_none()
        && provider.aws.is_none();
    let trusted_bedrock = matches!(
        provider_id,
        AMAZON_BEDROCK_PROVIDER_ID | AMAZON_BEDROCK_RUNTIME_PROVIDER_ID
    ) && provider.is_amazon_bedrock()
        && provider.aws.is_some()
        && provider.env_key.is_none()
        && provider.experimental_bearer_token.is_none()
        && provider.auth.is_none();
    trusted_openai || trusted_bedrock
}
