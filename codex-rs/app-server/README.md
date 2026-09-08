# Thread removal

`thread/archive` and `thread/delete` reject attempts to remove a live internal
worker with JSON-RPC error `-32600`. The worker's owner controls its shutdown.
For example, a Guardian reviewer remains available to its parent conversation
after a client tries to archive or delete it.

After the owner releases the worker, its saved conversation can be archived or
deleted normally. Ordinary client-controlled threads keep their existing behavior.

### Effective sampling settings

`turn/samplingSettingsEffective` reports the model provider, model, and reasoning
effort captured at a root thread sampling request boundary. The notification includes
`rootTurnId`, `samplingRequestId`, and a zero-based `attempt` counter so clients can
distinguish retries from subsequent requests. It describes the request sent by Codex;
it does not attest to provider-side model routing. These notifications are ephemeral
and are not restored from rollout history.
