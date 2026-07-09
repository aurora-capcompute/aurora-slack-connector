// Package connector bridges one Slack channel to a local aurora-dist runtime,
// turning it into an on-call duty bot.
//
// It is a pure client of aurora-dist's HTTP API — it embeds none of the
// runtime and has no dependency beyond the Go standard library. The shape of
// the integration:
//
//   - Each Slack thread is one aurora session (named "slack:<thread_ts>"), so a
//     thread reattaches to its session by name across connector restarts.
//   - Each user message runs as one process (an "aurora spawn") in that
//     session. A per-thread worker goroutine serializes them, honouring
//     aurora's one-active-process-per-session rule; because aurora seeds each
//     process with the session history, messages in a thread share context.
//   - While a process runs, the connector polls aurora and keeps a status
//     message current with the syscall timeline, then posts the answer.
//   - When a process parks on a pending approval task, the connector posts
//     Approve/Deny buttons; a click is verified, resolved through
//     POST /v1/tasks/{id}/resolve, and the parked process resumes.
//
// The connector holds no secrets of its own beyond the Slack credentials: the
// LLM endpoint and the capability grants live in the operator-supplied aurora
// manifest, which is forwarded to aurora verbatim.
//
// File layout:
//
//   - connector.go — the Connector type, lifecycle, HTTP mux, event ingestion
//     and routing, the per-thread worker, and process polling.
//   - approvals.go — human-in-the-loop: the interactions endpoint and the
//     approval prompt/resolution flow.
//   - aurora.go    — the aurora-dist HTTP client (sessions, processes, tasks).
//   - slack.go     — the Slack Web API client and request-signature verifier.
//   - progress.go  — rendering the syscall journal as a human status timeline.
//   - config.go    — environment configuration.
//   - seen.go      — bounded event de-duplication.
package connector
