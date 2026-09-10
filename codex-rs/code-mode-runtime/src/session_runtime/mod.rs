mod types;

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Instant;

use opentelemetry::context::FutureExt;
use serde_json::Value as JsonValue;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

pub(crate) use self::types::CellEvent;
pub(crate) use self::types::CellId;
pub(crate) use self::types::CreateCellRequest;
pub(crate) use self::types::Error;
pub(crate) use self::types::ImageDetail;
pub(crate) use self::types::NestedToolCall;
pub(crate) use self::types::ObserveMode;
pub(crate) use self::types::OutputItem;
pub(crate) use self::types::SessionRuntimeDelegate;
pub(crate) use self::types::ToolDefinition;
pub(crate) use self::types::ToolKind;
pub(crate) use self::types::ToolName;
use crate::TaskFailureHandler;
use crate::cell_actor::CellActor;
use crate::cell_actor::CellError;
use crate::cell_actor::CellEventFuture;
use crate::cell_actor::CellHandle;
use crate::cell_actor::CellHost;
use crate::cell_actor::CellState;
use crate::cell_actor::CellToolCall;
use crate::cell_actor::CompletionCommit;

type RuntimeEventFuture = Pin<Box<dyn Future<Output = Result<CellEvent, Error>> + Send + 'static>>;

/// Owns all cells and shared state for one transport-neutral code-mode session.
pub(crate) struct SessionRuntime<D: SessionRuntimeDelegate> {
    inner: Arc<Inner<D>>,
}

struct Inner<D: SessionRuntimeDelegate> {
    stored_values: Mutex<HashMap<String, JsonValue>>,
    cells: Mutex<HashMap<CellId, CellHandle>>,
    cell_tasks: TaskTracker,
    shutdown_token: CancellationToken,
    delegate: Arc<D>,
    task_failure_handler: Option<TaskFailureHandler>,
    next_cell_id: AtomicU64,
}

impl<D: SessionRuntimeDelegate> SessionRuntime<D> {
    pub(crate) fn new(delegate: Arc<D>) -> Self {
        Self::new_with_task_failure_handler(delegate, /*task_failure_handler*/ None)
    }

    pub(crate) fn new_with_task_failure_handler(
        delegate: Arc<D>,
        task_failure_handler: Option<TaskFailureHandler>,
    ) -> Self {
        Self {
            inner: Arc::new(Inner {
                stored_values: Mutex::new(HashMap::new()),
                cells: Mutex::new(HashMap::new()),
                cell_tasks: TaskTracker::new(),
                shutdown_token: CancellationToken::new(),
                delegate,
                task_failure_handler,
                next_cell_id: AtomicU64::new(1),
            }),
        }
    }

    pub(crate) async fn execute(
        &self,
        request: CreateCellRequest,
        initial_observe_mode: ObserveMode,
    ) -> Result<StartedCell, Error> {
        if self.inner.shutdown_token.is_cancelled() {
            return Err(Error::ShuttingDown);
        }
        let cell_id = self.allocate_cell_id()?;
        let tool_call_id = request.tool_call_id.clone();
        let initial_event = self
            .start_cell(cell_id.clone(), request, initial_observe_mode)
            .await?;
        tracing::info!(
            target: "codex_code_mode_runtime::lifecycle",
            cell_id = %cell_id,
            tool_call_id,
            observe_mode = ?initial_observe_mode,
            "code_mode_cell_started"
        );
        Ok(StartedCell {
            cell_id,
            initial_event,
            observe_started_at: Instant::now(),
        })
    }

    pub(crate) async fn observe(
        &self,
        cell_id: &CellId,
        mode: ObserveMode,
    ) -> Result<CellEvent, Error> {
        self.begin_observe(cell_id, mode).await?.event().await
    }

    pub(crate) async fn begin_observe(
        &self,
        cell_id: &CellId,
        mode: ObserveMode,
    ) -> Result<PendingEvent, Error> {
        let observe_started_at = Instant::now();
        tracing::info!(
            target: "codex_code_mode_runtime::lifecycle",
            cell_id = %cell_id,
            observe_mode = ?mode,
            "code_mode_observe_started"
        );
        let handle = self
            .inner
            .cells
            .lock()
            .await
            .get(cell_id)
            .cloned()
            .ok_or_else(|| Error::MissingCell(cell_id.clone()))?;
        Ok(PendingEvent {
            cell_id: cell_id.clone(),
            observe_mode: mode,
            observe_started_at,
            event: map_actor_event(cell_id.clone(), handle.observe(mode)),
        })
    }

    pub(crate) async fn terminate(&self, cell_id: &CellId) -> Result<CellEvent, Error> {
        let handle = self
            .inner
            .cells
            .lock()
            .await
            .get(cell_id)
            .cloned()
            .ok_or_else(|| Error::MissingCell(cell_id.clone()))?;
        handle
            .terminate()
            .await
            .map_err(|error| actor_error(cell_id, error))
    }

    pub(crate) async fn shutdown(&self) -> Result<(), Error> {
        self.begin_shutdown();
        // Taking the registry lock ensures every cell that passed the shutdown
        // check has registered its actor with the tracker before we wait.
        let cells = self.inner.cells.lock().await;
        self.inner.cell_tasks.close();
        drop(cells);
        self.inner.cell_tasks.wait().await;
        Ok(())
    }

    fn allocate_cell_id(&self) -> Result<CellId, Error> {
        self.inner
            .next_cell_id
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |next_cell_id| {
                next_cell_id.checked_add(1)
            })
            .map(|cell_id| CellId::new(cell_id.to_string()))
            .map_err(|_| Error::CellIdSpaceExhausted)
    }

    async fn start_cell(
        &self,
        cell_id: CellId,
        request: CreateCellRequest,
        initial_observe_mode: ObserveMode,
    ) -> Result<RuntimeEventFuture, Error> {
        let stored_values = self.inner.stored_values.lock().await.clone();
        let host = Arc::new(RuntimeCellHost {
            cell_id: cell_id.clone(),
            inner: Arc::clone(&self.inner),
            execution_context: opentelemetry::Context::current(),
        });
        let mut cells = self.inner.cells.lock().await;
        if self.inner.shutdown_token.is_cancelled() {
            return Err(Error::ShuttingDown);
        }
        if cells.contains_key(&cell_id) {
            return Err(Error::DuplicateCell(cell_id));
        }
        let cell_state = Arc::new(CellState::new(self.inner.shutdown_token.child_token()));
        let (handle, initial_event, task) = CellActor::prepare(
            cell_id.clone(),
            request,
            stored_values,
            host,
            initial_observe_mode,
            cell_state,
            self.inner.task_failure_handler.clone(),
        )
        .map_err(Error::Runtime)?;
        cells.insert(cell_id.clone(), handle);
        let task = self.inner.cell_tasks.spawn(task);
        if let Some(task_failure_handler) = self.inner.task_failure_handler.clone() {
            let failed_cell_id = cell_id.clone();
            let _failure_watcher = self.inner.cell_tasks.spawn(async move {
                if let Err(err) = task.await {
                    task_failure_handler(format!(
                        "code-mode cell {failed_cell_id} task failed: {err}"
                    ));
                }
            });
        }
        drop(cells);
        Ok(map_actor_event(cell_id, initial_event))
    }

    fn begin_shutdown(&self) {
        self.inner.shutdown_token.cancel();
        self.inner.cell_tasks.close();
    }
}

impl<D: SessionRuntimeDelegate> Drop for SessionRuntime<D> {
    fn drop(&mut self) {
        self.begin_shutdown();
    }
}

/// A cell admitted by [`SessionRuntime::execute`].
pub(crate) struct StartedCell {
    pub(crate) cell_id: CellId,
    initial_event: RuntimeEventFuture,
    observe_started_at: Instant,
}

impl StartedCell {
    pub(crate) async fn initial_event(self) -> Result<CellEvent, Error> {
        let result = self.initial_event.await;
        tracing::info!(
            target: "codex_code_mode_runtime::lifecycle",
            cell_id = %self.cell_id,
            result_class = cell_event_result_class(&result),
            elapsed_ms = elapsed_millis(self.observe_started_at),
            "code_mode_observe_resolved"
        );
        result
    }
}

/// An admitted observation that has not reached its requested frontier yet.
pub(crate) struct PendingEvent {
    cell_id: CellId,
    observe_mode: ObserveMode,
    observe_started_at: Instant,
    event: RuntimeEventFuture,
}

impl PendingEvent {
    pub(crate) async fn event(self) -> Result<CellEvent, Error> {
        let result = self.event.await;
        tracing::info!(
            target: "codex_code_mode_runtime::lifecycle",
            cell_id = %self.cell_id,
            observe_mode = ?self.observe_mode,
            result_class = cell_event_result_class(&result),
            elapsed_ms = elapsed_millis(self.observe_started_at),
            "code_mode_observe_resolved"
        );
        result
    }
}

struct RuntimeCellHost<D: SessionRuntimeDelegate> {
    cell_id: CellId,
    inner: Arc<Inner<D>>,
    // Callbacks outlive the initial request and run in separate tasks. Preserve
    // their trace parent without retaining the request's tracing span.
    execution_context: opentelemetry::Context,
}

impl<D: SessionRuntimeDelegate> CellHost for RuntimeCellHost<D> {
    async fn invoke_tool(
        &self,
        invocation: CellToolCall,
        cancellation_token: CancellationToken,
    ) -> Result<JsonValue, String> {
        let started_at = Instant::now();
        let runtime_tool_call_id = invocation.id.clone();
        let tool_name = invocation.name.name.clone();
        let tool_namespace = invocation.name.namespace.clone();
        let tool_kind = invocation.kind;
        tracing::info!(
            target: "codex_code_mode_runtime::lifecycle",
            cell_id = %self.cell_id,
            runtime_tool_call_id = %runtime_tool_call_id,
            tool_name,
            tool_namespace,
            ?tool_kind,
            "code_mode_tool_started"
        );
        let result = self
            .inner
            .delegate
            .invoke_tool(
                NestedToolCall {
                    cell_id: self.cell_id.clone(),
                    runtime_tool_call_id: invocation.id,
                    tool_name: invocation.name,
                    tool_kind: invocation.kind,
                    input: invocation.input,
                },
                cancellation_token,
            )
            .with_context(self.execution_context.clone())
            .await;
        let (result_class, output_bytes) = match &result {
            Ok(value) => (
                "success",
                serde_json::to_vec(value).map_or(0, |bytes| bytes.len()),
            ),
            Err(error_text) => ("error", error_text.len()),
        };
        tracing::info!(
            target: "codex_code_mode_runtime::lifecycle",
            cell_id = %self.cell_id,
            runtime_tool_call_id = %runtime_tool_call_id,
            tool_name,
            tool_namespace,
            ?tool_kind,
            result_class,
            output_bytes,
            elapsed_ms = elapsed_millis(started_at),
            "code_mode_tool_finished"
        );
        result
    }

    async fn notify(
        &self,
        call_id: String,
        text: String,
        cancellation_token: CancellationToken,
    ) -> Result<(), String> {
        self.inner
            .delegate
            .notify(call_id, self.cell_id.clone(), text, cancellation_token)
            .await
    }

    async fn commit_completion(
        &self,
        stored_value_writes: HashMap<String, JsonValue>,
        event: CellEvent,
        pending_initial_yield_items: Option<Vec<OutputItem>>,
        cell_state: Arc<CellState>,
    ) -> CompletionCommit {
        let event_class = match &event {
            CellEvent::Yielded { .. } => "yielded",
            CellEvent::Pending { .. } => "pending",
            CellEvent::Completed { .. } => "completed",
            CellEvent::Terminated { .. } => "terminated",
        };
        let pending_initial_yield_count = pending_initial_yield_items.as_ref().map_or(0, Vec::len);
        let started_at = Instant::now();
        let cancellation_token = cell_state.cancellation_token();
        let mut stored_values = tokio::select! {
            biased;
            _ = cancellation_token.cancelled() => {
                tracing::info!(
                    target: "codex_code_mode_runtime::lifecycle",
                    cell_id = %self.cell_id,
                    event_class,
                    pending_initial_yield_count,
                    accepted = false,
                    elapsed_ms = elapsed_millis(started_at),
                    "code_mode_completion_committed"
                );
                return CompletionCommit::Rejected(event);
            }
            stored_values = self.inner.stored_values.lock() => stored_values,
        };
        let commit = cell_state.commit_completion(event, pending_initial_yield_items, || {
            stored_values.extend(stored_value_writes);
        });
        tracing::info!(
            target: "codex_code_mode_runtime::lifecycle",
            cell_id = %self.cell_id,
            event_class,
            pending_initial_yield_count,
            accepted = matches!(&commit, CompletionCommit::Committed),
            elapsed_ms = elapsed_millis(started_at),
            "code_mode_completion_committed"
        );
        commit
    }

    async fn closed(&self) {
        let registry_removed = self
            .inner
            .cells
            .lock()
            .await
            .remove(&self.cell_id)
            .is_some();
        self.inner.delegate.cell_closed(&self.cell_id);
        tracing::info!(
            target: "codex_code_mode_runtime::lifecycle",
            cell_id = %self.cell_id,
            registry_removed,
            "code_mode_cell_closed"
        );
    }
}

fn map_actor_event(cell_id: CellId, event: CellEventFuture) -> RuntimeEventFuture {
    Box::pin(async move { event.await.map_err(|error| actor_error(&cell_id, error)) })
}

fn cell_event_result_class(result: &Result<CellEvent, Error>) -> &str {
    match result {
        Ok(CellEvent::Yielded { .. }) => "yielded",
        Ok(CellEvent::Pending { .. }) => "pending",
        Ok(CellEvent::Completed { .. }) => "completed",
        Ok(CellEvent::Terminated { .. }) => "terminated",
        Err(_) => "error",
    }
}

fn elapsed_millis(started_at: Instant) -> u64 {
    u64::try_from(started_at.elapsed().as_millis()).unwrap_or(u64::MAX)
}

fn actor_error(cell_id: &CellId, error: CellError) -> Error {
    match error {
        CellError::Busy => Error::BusyObserver(cell_id.clone()),
        CellError::AlreadyTerminating => Error::AlreadyTerminating(cell_id.clone()),
        CellError::Closed => Error::ClosedCell(cell_id.clone()),
    }
}

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
