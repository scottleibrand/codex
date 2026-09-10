//! TUI model and collaboration inventories; refreshing models preserves the server mode catalog.

use codex_protocol::config_types::CollaborationModeMask;
use codex_protocol::openai_models::ModelPreset;
use std::convert::Infallible;

pub(crate) const LUNA_RESERVE_MODEL: &str = "gpt-reserve";
pub(crate) const LUNA_MODEL: &str = "gpt-5.6-luna";

pub(crate) fn model_display_name(model: &str) -> &str {
    if model.eq_ignore_ascii_case(LUNA_RESERVE_MODEL) {
        "Luna Reserve"
    } else {
        let slug = model.rsplit('/').next().unwrap_or(model);
        if slug.eq_ignore_ascii_case("glm-5p3-flash") {
            "GLM 5.3 Flash"
        } else if slug.eq_ignore_ascii_case("glm-5p3") {
            "GLM 5.3"
        } else {
            model
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ModelCatalog {
    pub(crate) models: Vec<ModelPreset>,
    pub(crate) collaboration_modes: Vec<CollaborationModeMask>,
}

impl ModelCatalog {
    pub(crate) fn new(models: Vec<ModelPreset>) -> Self {
        Self {
            models,
            collaboration_modes: Vec::new(),
        }
    }

    pub(crate) fn with_collaboration_modes(mut self, modes: Vec<CollaborationModeMask>) -> Self {
        self.collaboration_modes = modes;
        self
    }

    pub(crate) fn try_list_models(&self) -> Result<Vec<ModelPreset>, Infallible> {
        Ok(self.models.clone())
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn model_display_name_maps_fireworks_slugs() {
        assert_eq!(
            model_display_name("accounts/fireworks/models/glm-5p3-flash"),
            "GLM 5.3 Flash"
        );
        assert_eq!(
            model_display_name("accounts/fireworks/models/glm-5p3"),
            "GLM 5.3"
        );
        assert_eq!(model_display_name("glm-5p3-flash"), "GLM 5.3 Flash");
        assert_eq!(model_display_name("gpt-5.6-luna"), "gpt-5.6-luna");
    }
}
