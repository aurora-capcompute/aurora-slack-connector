# aurora-slack-connector

A small HTTP server that turns one Slack channel into an on-call **duty bot**
backed by a local [`aurora-dist`](https://github.com/aurora-capcompute/aurora-dist)
runtime. Mention it in the channel and it opens an aurora session for the
thread, runs your message as a process (an "aurora spawn"), and narrates the
syscalls back into the thread while it works. Follow-up messages in the thread
run as further processes in the **same session**, so the investigation shares
history — the bot remembers what it already found.

Its purpose, for now, is a duty bot that **gathers information to help a human
mitigate an incident**: ask it a question, watch it pull from the internet,
memory, and its own reasoning, and get a written answer in the thread. When it
wants to take a sensitive action, it **asks first** — a human approves or denies
right in the thread (see [Human-in-the-loop](#human-in-the-loop)).

```
Slack events ──▶ ┌───────────────────────┐ ──localhost /v1──▶ aurora-dist
Slack actions ─▶ │ aurora-slack-connector │ ◀── poll: journal, tasks
     ▲           └───────────────────────┘
     └──── chat.postMessage / update / Approve·Deny buttons (in-thread) ──┘
```

The connector is a standalone HTTP server that **integrates with aurora-dist's
existing HTTP API over localhost** — it does not embed or vendor the runtime.
aurora-dist runs as its own process on the same machine; the connector is a
pure client of its `/v1` API (the module has zero non-stdlib dependencies).

## How it maps onto aurora

The connector is deliberately thin. All the intelligence, capabilities, and
secrets live in aurora and the manifest you give it; the connector only wires
Slack threads to aurora sessions.

| Slack | aurora | Notes |
| --- | --- | --- |
| a thread | a **session** (`POST /v1/sessions`, named `slack:<thread_ts>`) | one thread ⇄ one session |
| a message in that thread | a **process** (`POST /v1/sessions/{id}/processes`) | the message text is the process input |
| the bot's reply | the process's answer, polled to completion | posted back into the thread |
| "what is it doing?" | the process **journal** (`GET /v1/sessions/{id}`) | each `syscall.name` narrated as a status line |
| an approval prompt | a pending **task** (`…/tasks[]`) resolved via `POST /v1/tasks/{id}/resolve` | Approve/Deny buttons in the thread |

**Shared history.** aurora seeds every new root process with its session's
accumulated history and appends each completed process's `{input, answer}` back
onto it. So running every message in a thread as a process in that one session
*is* the shared-history behavior (`share_history` = true): message _n_ sees the
conversation of messages _1 … n-1_. The connector never starts a second session
for a thread — it reattaches to the existing one by name, even across restarts.

**Serialized per thread.** A session accepts only one active process at a time,
so the connector processes a thread's messages one at a time (queuing
follow-ups that arrive while a process is still running). Different threads run
concurrently.

**Progress by polling.** While a process runs, the connector polls aurora and
keeps a single status message updated with the syscall timeline — e.g.
`🧠 thinking`, `🌐 querying the internet …` (the `…` marks the syscall in
flight right now) — then posts the final answer as a new message.

## Human-in-the-loop

When a manifest grants a capability with `require_approval: true`, the agent
cannot use it unilaterally: aurora creates a pending **task** and parks the
process in `waiting_for_task` until a human decides. The connector turns that
into an in-thread prompt:

1. On seeing a pending approval task, it posts a Block Kit message —
   `🔐 Approval needed — the duty bot wants to …`, with the syscall and its
   arguments — and **Approve** / **Deny** buttons. The status line switches to
   `⏸️ waiting for your approval`, and the process's timeout is held open while
   a human thinks.
2. A click hits the connector's interactivity endpoint (`/slack/interactions`),
   whose signature is verified like any Slack request. The connector re-reads
   the task from the session log — for its current state and its
   `resolution_token` — and calls `POST /v1/tasks/{id}/resolve` with the
   decision and the clicking user as the `actor` (recorded on the resolution
   for the audit trail).
3. aurora resumes the parked process; the poll loop carries on to the answer.
   The prompt is rewritten to `✅ Approved by @you` / `🚫 Denied by @you` and its
   buttons removed, so it can't be actioned twice.

Notes:
- The **resolution token never travels through Slack** — only the (non-secret)
  task and session ids ride in the button; the token is fetched from aurora at
  click time. This also means the flow survives a connector restart.
- Timer parks (`sys.timer`) are *not* prompted — the runtime's timer actor
  resolves those itself; only genuine human-approval tasks raise a prompt.
- Any member of the channel can approve or deny; the decider is recorded.

## Slack app setup

Create a Slack app (from scratch) for your workspace and configure:

1. **OAuth scopes** (Bot Token Scopes): `app_mentions:read`, `channels:history`
   (or `groups:history` for a private channel), `chat:write`. Install the app
   and copy the **Bot User OAuth Token** (`xoxb-…`).
2. **Event Subscriptions**: enable, set the Request URL to
   `https://<your-host>/slack/events`, and subscribe to the bot events
   `app_mention` and `message.channels` (or `message.groups` for a private
   channel). Slack will call the URL once to verify it — the connector answers
   the handshake automatically.
3. **Interactivity & Shortcuts**: enable it and set the Request URL to
   `https://<your-host>/slack/interactions`. This is what delivers the
   Approve/Deny button clicks for human-in-the-loop approvals.
4. **Signing secret**: copy it from *Basic Information* → *App Credentials*. The
   connector verifies every request (events and interactions) against it.
5. Invite the bot to the one channel it should serve and note that channel's ID
   (`C…`).

The Events API delivers over HTTP, so the connector must be reachable by Slack
at a public HTTPS URL (put it behind your ingress / a tunnel in dev). Both the
events and interactions Request URLs point at the same server.

## Configuration

All configuration is by environment variable. The connector holds **no secrets
of its own** beyond the Slack credentials — the LLM endpoint, API key, and
capability grants live in the aurora manifest you supply.

| Variable | Required | Default | Meaning |
| --- | --- | --- | --- |
| `SLACK_BOT_TOKEN` | ✅ | — | Bot token (`xoxb-…`) for Web API calls |
| `SLACK_SIGNING_SECRET` | ✅ | — | Verifies inbound Slack requests |
| `SLACK_CHANNEL_ID` | ✅ | — | The single channel to serve (`C…`) |
| `AURORA_MANIFEST` *or* `AURORA_MANIFEST_FILE` | ✅ | — | The aurora manifest (inline JSON, or a path) applied to every process |
| `AURORA_BASE_URL` | | `http://localhost:8080` | The local aurora-dist address |
| `SLACK_TRIGGER_KEYWORD` | | `@duty` | Keyword that starts a new thread (a native @-mention always works too) |
| `SLACK_BOT_USER_ID` | | auto (`auth.test`) | The bot's own user id, for mention-stripping and self-filtering |
| `SLACK_API_BASE_URL` | | `https://slack.com/api` | Override for an enterprise gateway or testing |
| `ADDR` | | `:3000` | Listen address |
| `EVENTS_PATH` | | `/slack/events` | Path Slack posts events to |
| `INTERACTIONS_PATH` | | `/slack/interactions` | Path Slack posts button clicks to (must differ from `EVENTS_PATH`) |
| `POLL_INTERVAL` | | `2s` | How often a running process is polled |
| `PROCESS_TIMEOUT` | | `15m` | How long to actively report on one process before backing off |
| `HTTP_TIMEOUT` | | `30s` | Per-request timeout for aurora/Slack calls |

Durations accept a Go duration string (`2s`, `500ms`, `10m`) or a bare integer
meaning seconds.

### The manifest

The manifest is aurora's grant set for each process — it names the program, the
LLM cognition driver, and the leaf capabilities the duty bot may use. It is
passed through to aurora verbatim, so this is where you put the model endpoint
and key. A starting point for an information-gathering duty bot:

```json
{
  "version": 4,
  "syscalls": [
    {
      "syscall": "core.openaiApi",
      "hidden": true,
      "base_url": "https://api.openai.com/v1",
      "api_key": "sk-...",
      "default_model": "gpt-4o",
      "capabilities": [{ "operation": "chat", "require_approval": false }]
    },
    {
      "syscall": "core.internet",
      "capabilities": [{ "methods": ["GET"], "domain": "status.example.com" }]
    },
    { "syscall": "core.memory", "capabilities": [{ "operation": "get" }, { "operation": "set" }] },
    { "syscall": "sys.timer" }
  ]
}
```

Notes:
- Set `require_approval: true` on any capability whose use should need a human
  sign-off (e.g. a `POST` to an internal endpoint). aurora will park the process
  and the connector will raise an Approve/Deny prompt in the thread — see
  [Human-in-the-loop](#human-in-the-loop). Read-only lookups can stay
  `require_approval: false` so investigation flows without interruption.
- Add `sys.spawn` if you want the agent to delegate sub-investigations; its
  child processes inherit the session history by default (`history: true`).
- The manifest must not grant beyond aurora-dist's configured capability
  ceiling, or process creation is refused.

## Run

```sh
export SLACK_BOT_TOKEN=xoxb-...
export SLACK_SIGNING_SECRET=...
export SLACK_CHANNEL_ID=C0123456789
export AURORA_MANIFEST_FILE=./duty-manifest.json
export AURORA_BASE_URL=http://localhost:8080

go run ./cmd/aurora-slack-connector
# or: go build -o aurora-slack-connector ./cmd/aurora-slack-connector && ./aurora-slack-connector
```

Then in the channel:

> **@duty** why are we seeing elevated 500s on checkout?

The bot opens a thread, reports each step, and replies with what it found. Keep
replying in the thread to keep digging in the same session.

## Development

Pure Go standard library — no dependencies.

```sh
go vet ./...
go test -race ./...
go build ./...
```

The tests stub both aurora-dist and the Slack Web API with `httptest` servers
and drive the connector end to end (session creation, process spawn, journal
polling, progress updates, dedup of duplicate deliveries, signature
verification, thread → session reattachment, and the full approval flow —
prompt → signed button click → task resolution → answer).

## Scope and limitations

- **One channel, one connector.** By design.
- **Events API only.** The connector receives over HTTP; it needs a public URL
  (for both the events and interactions Request URLs).
- **Approvals are approve/deny.** Human-in-the-loop covers the common case — a
  gated syscall a human approves or denies. Richer resolutions aurora supports
  (e.g. `completed` with substituted data) aren't exposed as buttons. Timer
  parks resolve themselves and are polled through, never prompted.
- **In-memory dedup and worker registry.** These are rebuilt on restart; the
  durable thread → session mapping (the session name) and the tasks' aurora-side
  state are what preserve continuity across restarts.
