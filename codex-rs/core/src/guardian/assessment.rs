//! The production synchronous reviewer's output schema and strict JSON parser.
//! Parsing defaults and error text are shared unchanged by all reviewer callers.

use codex_protocol::protocol::GuardianAssessmentOutcome;
use codex_protocol::protocol::GuardianRiskLevel;
use codex_protocol::protocol::GuardianUserAuthorization;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;

/// Structured output contract that the guardian reviewer must satisfy.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct GuardianAssessment {
    pub risk_level: codex_protocol::protocol::GuardianRiskLevel,
    pub user_authorization: codex_protocol::protocol::GuardianUserAuthorization,
    pub outcome: GuardianAssessmentOutcome,
    pub rationale: String,
}

/// Only a complete JSON assessment object can authorize an action.
pub fn parse_guardian_assessment(text: Option<&str>) -> anyhow::Result<GuardianAssessment> {
    let Some(text) = text else {
        anyhow::bail!("guardian review completed without an assessment payload");
    };
    if !text.trim_start().starts_with('{') {
        anyhow::bail!("guardian assessment must be a JSON object");
    }
    let parsed_payload = serde_json::from_str::<GuardianAssessmentPayload>(text)?;

    let outcome = parsed_payload.outcome;
    let risk_level = parsed_payload.risk_level.unwrap_or(match outcome {
        GuardianAssessmentOutcome::Allow => GuardianRiskLevel::Low,
        GuardianAssessmentOutcome::Deny => GuardianRiskLevel::High,
    });
    let rationale = parsed_payload
        .rationale
        .filter(|rationale| !rationale.trim().is_empty())
        .unwrap_or_else(|| match outcome {
            GuardianAssessmentOutcome::Allow => {
                "Auto-review returned a low-risk allow decision.".to_string()
            }
            GuardianAssessmentOutcome::Deny => {
                "Auto-review returned a deny decision without a rationale.".to_string()
            }
        });

    Ok(GuardianAssessment {
        risk_level,
        user_authorization: parsed_payload
            .user_authorization
            .unwrap_or(GuardianUserAuthorization::Unknown),
        outcome,
        rationale,
    })
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct GuardianAssessmentPayload {
    risk_level: Option<GuardianRiskLevel>,
    user_authorization: Option<GuardianUserAuthorization>,
    outcome: GuardianAssessmentOutcome,
    rationale: Option<String>,
}

/// JSON schema supplied as `final_output_json_schema` to guide a structured
/// final answer from the guardian review session.
///
/// Keep this next to `guardian_output_contract_prompt()` so the prompt text and
/// output schema stay aligned.
pub fn guardian_output_schema() -> Value {
    serde_json::json!({
        "type": "object",
        "additionalProperties": false,
        "properties": {
            "risk_level": {
                "type": "string",
                "enum": ["low", "medium", "high", "critical"]
            },
            "user_authorization": {
                "type": "string",
                "enum": ["unknown", "low", "medium", "high"]
            },
            "outcome": {
                "type": "string",
                "enum": ["allow", "deny"]
            },
            "rationale": {
                "type": "string"
            }
        },
        "required": ["outcome"]
    })
}

/// Prompt fragment that describes the exact JSON contract paired with
/// `guardian_output_schema()`.
pub(super) fn guardian_output_contract_prompt() -> &'static str {
    r#"You may use read-only tool checks to gather any additional context you need before deciding. When you are ready to answer, your final message must be strict JSON.

For low-risk actions, give the final answer directly: {"outcome":"allow"}.

For anything else, use this JSON schema:
{
  "risk_level": "low" | "medium" | "high" | "critical",
  "user_authorization": "unknown" | "low" | "medium" | "high",
  "outcome": "allow" | "deny",
  "rationale": string
}"#
}

#[cfg(test)]
#[path = "assessment_tests.rs"]
mod tests;
