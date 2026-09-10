use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::time::Instant;

use futures::FutureExt;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::warn;

use super::CellHost;
use super::CellToolCall;
use crate::TaskFailureHandler;
use crate::runtime::RuntimeCommand;
use crate::session_runtime::CellId;

#[derive(Clone, Copy, Debug)]
pub(super) enum CallbackCompletion {
    DrainNotifications,
    Cancel,
}

pub(super) fn spawn_notification<H: CellHost>(
    tasks: &mut JoinSet<()>,
    host: Arc<H>,
    call_id: String,
    text: String,
    cancellation_token: CancellationToken,
    task_failure_handler: Option<TaskFailureHandler>,
) {
    tasks.spawn(async move {
        let callback =
            AssertUnwindSafe(async move { host.notify(call_id, text, cancellation_token).await })
                .catch_unwind()
                .await;
        match callback {
            Ok(Ok(())) => {}
            Ok(Err(err)) => warn!("failed to deliver code mode notification: {err}"),
            Err(_) => report_task_failure(
                task_failure_handler.as_ref(),
                "code mode notification task panicked".to_string(),
            ),
        }
    });
}

pub(super) fn spawn_tool<H: CellHost>(
    cell_id: CellId,
    tasks: &mut JoinSet<()>,
    host: Arc<H>,
    invocation: CellToolCall,
    runtime_tx: std::sync::mpsc::Sender<RuntimeCommand>,
    cancellation_token: CancellationToken,
    task_failure_handler: Option<TaskFailureHandler>,
) {
    tasks.spawn(async move {
        let runtime_tool_call_id = invocation.id.clone();
        let id = invocation.id.clone();
        let callback =
            AssertUnwindSafe(async move { host.invoke_tool(invocation, cancellation_token).await })
                .catch_unwind()
                .await;
        let (command, failure_reason) = match callback {
            Ok(Ok(result)) => (RuntimeCommand::ToolResponse { id, result }, None),
            Ok(Err(error_text)) => (RuntimeCommand::ToolError { id, error_text }, None),
            Err(_) => {
                let failure_reason = "code mode tool task panicked".to_string();
                (
                    RuntimeCommand::ToolError {
                        id,
                        error_text: failure_reason.clone(),
                    },
                    Some(failure_reason),
                )
            }
        };
        let command_kind = match &command {
            RuntimeCommand::ToolResponse { .. } => "tool_response",
            RuntimeCommand::ToolError { .. } => "tool_error",
            _ => "unexpected",
        };
        let send_succeeded = runtime_tx.send(command).is_ok();
        tracing::info!(
            target: "codex_code_mode_runtime::lifecycle",
            cell_id = %cell_id,
            runtime_tool_call_id = %runtime_tool_call_id,
            command_kind,
            send_succeeded,
            "code_mode_runtime_command_sent"
        );
        if let Some(failure_reason) = failure_reason {
            report_task_failure(task_failure_handler.as_ref(), failure_reason);
        }
    });
}

pub(super) async fn finish_callbacks(
    cell_id: &CellId,
    cancellation_token: &CancellationToken,
    notification_tasks: &mut JoinSet<()>,
    tool_tasks: &mut JoinSet<()>,
    completion: CallbackCompletion,
    task_failure_handler: Option<&TaskFailureHandler>,
) {
    let started_at = Instant::now();
    let notification_task_count = notification_tasks.len();
    let tool_task_count = tool_tasks.len();
    tracing::info!(
        target: "codex_code_mode_runtime::lifecycle",
        cell_id = %cell_id,
        ?completion,
        notification_task_count,
        tool_task_count,
        cancellation_requested = cancellation_token.is_cancelled(),
        "code_mode_callback_drain_started"
    );
    if matches!(completion, CallbackCompletion::Cancel) {
        cancellation_token.cancel();
    }
    drain_tasks(notification_tasks, "notification", task_failure_handler).await;
    cancellation_token.cancel();
    drain_tasks(tool_tasks, "tool", task_failure_handler).await;
    tracing::info!(
        target: "codex_code_mode_runtime::lifecycle",
        cell_id = %cell_id,
        ?completion,
        notification_task_count,
        tool_task_count,
        elapsed_ms = u64::try_from(started_at.elapsed().as_millis()).unwrap_or(u64::MAX),
        cancellation_requested = cancellation_token.is_cancelled(),
        "code_mode_callback_drain_finished"
    );
}

pub(super) fn report_task_result(
    task_result: Option<Result<(), tokio::task::JoinError>>,
    description: &str,
    task_failure_handler: Option<&TaskFailureHandler>,
) {
    if let Some(Err(err)) = task_result
        && !err.is_cancelled()
    {
        report_task_failure(
            task_failure_handler,
            format!("code mode {description} task failed: {err}"),
        );
    }
}

fn report_task_failure(task_failure_handler: Option<&TaskFailureHandler>, failure_reason: String) {
    warn!("{failure_reason}");
    if let Some(task_failure_handler) = task_failure_handler {
        task_failure_handler(failure_reason);
    }
}

async fn drain_tasks(
    tasks: &mut JoinSet<()>,
    description: &str,
    task_failure_handler: Option<&TaskFailureHandler>,
) {
    while let Some(result) = tasks.join_next().await {
        report_task_result(Some(result), description, task_failure_handler);
    }
}

#[cfg(test)]
#[path = "callbacks_tests.rs"]
mod tests;
