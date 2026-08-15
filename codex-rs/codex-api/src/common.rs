use crate::error::ApiError;
use codex_protocol::config_types::ReasoningSummary as ReasoningSummaryConfig;
use codex_protocol::config_types::Verbosity as VerbosityConfig;
use codex_protocol::models::ResponseItem;
use codex_protocol::openai_models::ReasoningEffort as ReasoningEffortConfig;
use codex_protocol::protocol::ModelVerification;
use codex_protocol::protocol::RateLimitSnapshot;
use codex_protocol::protocol::TokenUsage;
use codex_protocol::protocol::TurnModerationMetadataEvent;
use codex_protocol::protocol::W3cTraceContext;
use futures::Stream;
use serde::Deserialize;
use serde::Serialize;
use serde::ser::Error as _;
use serde::ser::SerializeSeq;
use serde_json::Value;
use serde_json::value::RawValue;
use std::collections::HashMap;
use std::ops::Deref;
use std::ops::DerefMut;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;
use tokio::sync::mpsc;

pub const WS_REQUEST_HEADER_TRACEPARENT_CLIENT_METADATA_KEY: &str = "ws_request_header_traceparent";
pub const WS_REQUEST_HEADER_TRACESTATE_CLIENT_METADATA_KEY: &str = "ws_request_header_tracestate";

/// Canonical input payload for the compaction endpoint.
#[derive(Debug, Clone, Serialize)]
pub struct CompactionInput<'a> {
    pub model: &'a str,
    pub input: &'a [ResponseItem],
    #[serde(skip_serializing_if = "str::is_empty")]
    pub instructions: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tools: Option<ResponsesApiTools>,
    pub parallel_tool_calls: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reasoning: Option<Reasoning>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub service_tier: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prompt_cache_key: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub text: Option<TextControls>,
}

/// Canonical input payload for the memory summarize endpoint.
#[derive(Debug, Clone, Serialize)]
pub struct MemorySummarizeInput {
    pub model: String,
    #[serde(rename = "traces")]
    pub raw_memories: Vec<RawMemory>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reasoning: Option<Reasoning>,
}

#[derive(Debug, Clone, Serialize)]
pub struct RawMemory {
    pub id: String,
    pub metadata: RawMemoryMetadata,
    pub items: Vec<Value>,
}

#[derive(Debug, Clone, Serialize)]
pub struct RawMemoryMetadata {
    pub source_path: String,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct MemorySummarizeOutput {
    #[serde(rename = "trace_summary", alias = "raw_memory")]
    pub raw_memory: String,
    pub memory_summary: String,
}

#[derive(Debug)]
pub enum ResponseEvent {
    Created,
    SafetyBuffering(SafetyBuffering),
    OutputItemDone(ResponseItem),
    OutputItemAdded(ResponseItem),
    /// Emitted when the server includes `OpenAI-Model` on the stream response.
    /// This can differ from the requested model when backend safety routing applies.
    ServerModel(String),
    /// Emitted when the server recommends additional account verification.
    ModelVerifications(Vec<ModelVerification>),
    /// Emitted when the server includes moderation metadata for first-party turn presentation.
    TurnModerationMetadata(TurnModerationMetadataEvent),
    /// Emitted when `X-Reasoning-Included: true` is present on the response,
    /// meaning the server already accounted for past reasoning tokens and the
    /// client should not re-estimate them.
    ServerReasoningIncluded(bool),
    Completed {
        response_id: String,
        token_usage: Option<TokenUsage>,
        /// Did the model affirmatively end its turn? Some providers do not set this,
        /// so we rely on fallback logic when this is `None`.
        end_turn: Option<bool>,
    },
    OutputTextDelta(String),
    ToolCallInputDelta {
        item_id: String,
        call_id: Option<String>,
        delta: String,
    },
    ReasoningSummaryDelta {
        delta: String,
        summary_index: i64,
    },
    ReasoningSummaryDone {
        item_id: String,
        text: String,
        summary_index: i64,
    },
    ReasoningContentDelta {
        delta: String,
        content_index: i64,
    },
    ReasoningSummaryPartAdded {
        summary_index: i64,
    },
    RateLimits(RateLimitSnapshot),
    ModelsEtag(String),
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct SafetyBuffering {
    pub use_cases: Vec<String>,
    pub reasons: Vec<String>,
    #[serde(skip)]
    pub show_buffering_ui: bool,
    #[serde(rename = "retry_model")]
    pub faster_model: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct SafetyBufferingTreatment {
    pub faster_model: Option<String>,
}

#[derive(Debug, Serialize, Clone, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum ReasoningContext {
    Auto,
    CurrentTurn,
    AllTurns,
}

#[derive(Debug, Serialize, Clone, PartialEq)]
pub struct Reasoning {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub effort: Option<ReasoningEffortConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub summary: Option<ReasoningSummaryConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub context: Option<ReasoningContext>,
}

#[derive(Debug, Serialize, Clone, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum ReasoningSummaryDelivery {
    SequentialCutoff,
}

#[derive(Debug, Serialize, Clone, PartialEq)]
pub struct StreamOptions {
    pub reasoning_summary_delivery: ReasoningSummaryDelivery,
}

#[derive(Debug, Serialize, Default, Clone, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum TextFormatType {
    #[default]
    JsonSchema,
}

#[derive(Debug, Serialize, Default, Clone, PartialEq)]
pub struct TextFormat {
    /// Format type used by the OpenAI text controls.
    pub r#type: TextFormatType,
    /// When true, the server is expected to strictly validate responses.
    pub strict: bool,
    /// JSON schema for the desired output.
    pub schema: Value,
    /// Friendly name for the format, used in telemetry/debugging.
    pub name: String,
}

/// Controls the `text` field for the Responses API, combining verbosity and
/// optional JSON schema output formatting.
#[derive(Debug, Serialize, Default, Clone, PartialEq)]
pub struct TextControls {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub verbosity: Option<OpenAiVerbosity>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub format: Option<TextFormat>,
}

#[derive(Debug, Serialize, Default, Clone, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum OpenAiVerbosity {
    Low,
    #[default]
    Medium,
    High,
}

#[derive(Debug, Serialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum PromptCacheMode {
    Implicit,
    Explicit,
}

#[derive(Debug, Serialize, Clone, Copy, PartialEq, Eq)]
pub struct PromptCacheOptions {
    pub mode: PromptCacheMode,
}

impl From<VerbosityConfig> for OpenAiVerbosity {
    fn from(v: VerbosityConfig) -> Self {
        match v {
            VerbosityConfig::Low => OpenAiVerbosity::Low,
            VerbosityConfig::Medium => OpenAiVerbosity::Medium,
            VerbosityConfig::High => OpenAiVerbosity::High,
        }
    }
}

/// Serialized tool definitions for Responses API requests.
///
/// Keeping the tool list as raw JSON avoids rebuilding a generic JSON value
/// tree, while the shared allocation keeps request clones cheap.
#[derive(Debug, Clone)]
pub struct ResponsesApiTools(Arc<RawValue>);

impl ResponsesApiTools {
    pub(crate) fn as_raw_value(&self) -> &RawValue {
        &self.0
    }
}

impl From<Arc<RawValue>> for ResponsesApiTools {
    fn from(value: Arc<RawValue>) -> Self {
        Self(value)
    }
}

impl PartialEq for ResponsesApiTools {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0) || self.0.get() == other.0.get()
    }
}

impl Serialize for ResponsesApiTools {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.0.serialize(serializer)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ResponsesApiInput {
    items: Vec<ResponseItem>,
    prompt_cache_breakpoints: Vec<usize>,
}

impl ResponsesApiInput {
    pub fn with_prompt_cache_breakpoints(
        items: Vec<ResponseItem>,
        prompt_cache_breakpoints: Vec<usize>,
    ) -> Self {
        Self {
            items,
            prompt_cache_breakpoints,
        }
    }

    pub fn prompt_cache_breakpoints(&self) -> &[usize] {
        &self.prompt_cache_breakpoints
    }
}

impl From<Vec<ResponseItem>> for ResponsesApiInput {
    fn from(items: Vec<ResponseItem>) -> Self {
        Self::with_prompt_cache_breakpoints(items, Vec::new())
    }
}

impl Deref for ResponsesApiInput {
    type Target = Vec<ResponseItem>;

    fn deref(&self) -> &Self::Target {
        &self.items
    }
}

impl DerefMut for ResponsesApiInput {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.items
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ResponsesApiInputRef<'a> {
    items: &'a [ResponseItem],
    prompt_cache_breakpoints: &'a [usize],
}

impl<'a> ResponsesApiInputRef<'a> {
    pub fn new(items: &'a [ResponseItem], prompt_cache_breakpoints: &'a [usize]) -> Self {
        Self {
            items,
            prompt_cache_breakpoints,
        }
    }
}

fn insert_prompt_cache_breakpoint(wire_item: &mut Value) -> Result<(), &'static str> {
    let item_type = wire_item.get("type").and_then(Value::as_str);
    let content = match item_type {
        Some("message") => wire_item.get_mut("content"),
        Some("function_call_output") => {
            let output = wire_item
                .get_mut("output")
                .ok_or("function call output is missing output")?;
            if let Some(text) = output.as_str().map(str::to_owned) {
                *output = serde_json::json!([{"type": "input_text", "text": text}]);
            }
            Some(output)
        }
        _ => None,
    }
    .and_then(Value::as_array_mut)
    .ok_or("prompt cache breakpoint must target message or function output content")?;
    let block = content
        .iter_mut()
        .rev()
        .find(|block| {
            matches!(
                block.get("type").and_then(Value::as_str),
                Some("input_text" | "input_image" | "input_file")
            )
        })
        .and_then(Value::as_object_mut)
        .ok_or("prompt cache breakpoint requires supported input content")?;
    block.insert(
        "prompt_cache_breakpoint".to_string(),
        serde_json::json!({"mode": "explicit"}),
    );
    Ok(())
}

fn serialize_responses_api_input<S>(
    items: &[ResponseItem],
    prompt_cache_breakpoints: &[usize],
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    let mut sequence = serializer.serialize_seq(Some(items.len()))?;
    for (index, item) in items.iter().enumerate() {
        if prompt_cache_breakpoints.contains(&index) {
            let mut wire_item = serde_json::to_value(item).map_err(S::Error::custom)?;
            insert_prompt_cache_breakpoint(&mut wire_item).map_err(S::Error::custom)?;
            sequence.serialize_element(&wire_item)?;
        } else {
            sequence.serialize_element(item)?;
        }
    }
    sequence.end()
}

impl Serialize for ResponsesApiInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serialize_responses_api_input(&self.items, &self.prompt_cache_breakpoints, serializer)
    }
}

impl Serialize for ResponsesApiInputRef<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serialize_responses_api_input(self.items, self.prompt_cache_breakpoints, serializer)
    }
}

#[derive(Debug, Serialize, Clone, PartialEq)]
pub struct ResponsesApiRequest {
    pub model: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub instructions: String,
    pub input: ResponsesApiInput,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tools: Option<ResponsesApiTools>,
    pub tool_choice: String,
    pub parallel_tool_calls: bool,
    pub reasoning: Option<Reasoning>,
    pub store: bool,
    pub stream: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stream_options: Option<StreamOptions>,
    pub include: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub service_tier: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prompt_cache_key: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prompt_cache_options: Option<PromptCacheOptions>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub text: Option<TextControls>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_metadata: Option<HashMap<String, String>>,
}

impl<'a> From<&'a ResponsesApiRequest> for ResponseCreateWsRequest<'a> {
    fn from(request: &'a ResponsesApiRequest) -> Self {
        Self {
            model: &request.model,
            instructions: &request.instructions,
            previous_response_id: None,
            input: ResponsesApiInputRef::new(
                &request.input,
                request.input.prompt_cache_breakpoints(),
            ),
            tools: request.tools.as_ref().map(ResponsesApiTools::as_raw_value),
            tool_choice: &request.tool_choice,
            parallel_tool_calls: request.parallel_tool_calls,
            reasoning: request.reasoning.as_ref(),
            store: request.store,
            stream: request.stream,
            stream_options: request.stream_options.as_ref(),
            include: &request.include,
            service_tier: request.service_tier.as_deref(),
            prompt_cache_key: request.prompt_cache_key.as_deref(),
            prompt_cache_options: request.prompt_cache_options.as_ref(),
            text: request.text.as_ref(),
            generate: None,
            client_metadata: request.client_metadata.clone(),
        }
    }
}

#[derive(Debug, Serialize)]
pub struct ResponseCreateWsRequest<'a> {
    pub model: &'a str,
    #[serde(skip_serializing_if = "str::is_empty")]
    pub instructions: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub previous_response_id: Option<String>,
    pub input: ResponsesApiInputRef<'a>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tools: Option<&'a RawValue>,
    pub tool_choice: &'a str,
    pub parallel_tool_calls: bool,
    pub reasoning: Option<&'a Reasoning>,
    pub store: bool,
    pub stream: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stream_options: Option<&'a StreamOptions>,
    pub include: &'a [String],
    #[serde(skip_serializing_if = "Option::is_none")]
    pub service_tier: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prompt_cache_key: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prompt_cache_options: Option<&'a PromptCacheOptions>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub text: Option<&'a TextControls>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub generate: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_metadata: Option<HashMap<String, String>>,
}

pub fn response_create_client_metadata(
    client_metadata: Option<HashMap<String, String>>,
    trace: Option<&W3cTraceContext>,
) -> Option<HashMap<String, String>> {
    let mut client_metadata = client_metadata.unwrap_or_default();

    if let Some(traceparent) = trace.and_then(|trace| trace.traceparent.as_deref()) {
        client_metadata.insert(
            WS_REQUEST_HEADER_TRACEPARENT_CLIENT_METADATA_KEY.to_string(),
            traceparent.to_string(),
        );
    }
    if let Some(tracestate) = trace.and_then(|trace| trace.tracestate.as_deref()) {
        client_metadata.insert(
            WS_REQUEST_HEADER_TRACESTATE_CLIENT_METADATA_KEY.to_string(),
            tracestate.to_string(),
        );
    }

    (!client_metadata.is_empty()).then_some(client_metadata)
}

#[derive(Debug, Serialize)]
#[serde(tag = "type")]
#[allow(clippy::large_enum_variant)]
pub enum ResponsesWsRequest<'a> {
    #[serde(rename = "response.create")]
    ResponseCreate(ResponseCreateWsRequest<'a>),
}

pub fn create_text_param_for_request(
    verbosity: Option<VerbosityConfig>,
    output_schema: &Option<Value>,
    output_schema_strict: bool,
) -> Option<TextControls> {
    if verbosity.is_none() && output_schema.is_none() {
        return None;
    }

    Some(TextControls {
        verbosity: verbosity.map(std::convert::Into::into),
        format: output_schema.as_ref().map(|schema| TextFormat {
            r#type: TextFormatType::JsonSchema,
            strict: output_schema_strict,
            schema: schema.clone(),
            name: "codex_output_schema".to_string(),
        }),
    })
}

pub struct ResponseStream {
    pub rx_event: mpsc::Receiver<Result<ResponseEvent, ApiError>>,
    /// Server-assigned `x-request-id` response header, when present.
    pub upstream_request_id: Option<String>,
}

impl Stream for ResponseStream {
    type Item = Result<ResponseEvent, ApiError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.rx_event.poll_recv(cx)
    }
}
