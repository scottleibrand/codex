use thiserror::Error;

/// Error returned while executing a model-visible tool invocation.
#[derive(Debug, Error, PartialEq)]
pub enum FunctionCallError {
    #[error("{0}")]
    RespondToModel(String),
    #[error("{0}")]
    MalformedArguments(String),
    #[error("Fatal error: {0}")]
    Fatal(String),
}
