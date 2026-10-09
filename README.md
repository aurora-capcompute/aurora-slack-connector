# aurora-slack-connector

A service that turns one Slack channel into a duty bot backed by an
[aurora-dist](https://github.com/aurora-capcompute/aurora-dist) server. Mention
the bot, and it opens an Aurora session for the thread, runs your message as a
process, and reports each syscall in the thread while it works. Later messages
in the thread run as more processes in the same session.

The bot gathers information to help a human handle an incident. When it needs a
sensitive action, it asks first: a human approves or denies in the thread.

```
Slack ── events, clicks (Socket Mode) ──▶ ┌────────────────────────┐ ── /v1 ──▶ aurora-dist
  ▲                                       │ aurora-slack-connector │ ◀── poll: journal, tasks
  └───── chat.postMessage, chat.update ── └────────────────────────┘
```

The connector is a client of aurora-dist's `/v1` API. It does not embed the
runtime. Slack reaches it over Socket Mode: the connector opens a WebSocket to
Slack, so it needs no public URL and listens on no port. Its one dependency is
the WebSocket client. The agent, its capabilities and its secrets live in Aurora
and in the manifest you give it.

## How Slack maps onto Aurora

| Slack | Aurora |
| --- | --- |
| a thread | a session named `slack:<thread_ts>`, with tags `source`, `channel`, `thread_ts` |
| a message in the thread | a process. The message text is its input. |
| the bot's reply | the process's answer |
| "what is it doing?" | the process's journal. Each syscall becomes a status line. |
| an approval prompt | a pending task, resolved with `POST /v1/tasks/{id}/resolve` |

- **Shared history.** Aurora gives each new process its session's history: the
  input and answer of each completed process before it. So message *n* sees
  messages *1 … n-1*, unless the manifest sets `"history": false`. The connector
  finds a thread's session by name, also after a restart.
- **One process per thread at a time.** A session runs one process at a time,
  so the connector queues later messages of a thread. Different threads run in
  parallel.
- **Progress by polling.** While a process runs, the connector polls Aurora and
  keeps one status message up to date: `🧠 Thinking`, `🌐 Reading
  status.example.com`, `🔌 Calling search_onyx`, `🖥️ Running kubectl-get`.
  A `…` marks the call in progress. Savepoints and other housekeeping calls
  are not shown. Then it posts the answer as a new message.
- **Interrupted runs.** A run can stop before it finishes: past the server's
  time limit (`quantum_deadline_seconds`), or cut off by a restart. The
  connector says so in the thread and posts a **Retry** button. A click resumes
  the run where its journal stopped.

## Approvals

A grant with `require_approval: true` makes Aurora park the process on a task
(`waiting_for_task`). The connector then:

1. Posts a prompt that says what the bot wants to do, shows the exact call
   (its arguments, or a command's resolved command line), shows when the
   request expires, and has **Approve** / **Deny** buttons. The status line
   shows `⏸️ waiting for your approval`, and the process timeout waits.
2. On a click, reads the task again from the session (its state and
   `resolution_token`), and resolves it with the decision. The clicking user is the `actor` in the audit trail.
3. Rewrites the prompt to `✅ Approved by @user` or `🚫 Denied by @user` and
   removes the buttons. Aurora resumes the process.

The resolution token never goes through Slack. The button carries only the
task and session ids, and the connector reads the token from Aurora at click
time. So the flow survives a connector restart. Timers (`sys.timer`) never
raise a prompt: the runtime fires them. Any channel member can approve or deny.

## Slack app setup

1. **Socket Mode:** turn it on. Create an app-level token with the
   `connections:write` scope, and copy it (`xapp-…`).
2. **Bot token scopes:** `app_mentions:read`, `channels:history` (or
   `groups:history` for a private channel), `chat:write`. Install the app and
   copy the bot token (`xoxb-…`).
3. **Event Subscriptions:** turn them on, and subscribe to `app_mention` and
   `message.channels` (or `message.groups`). Socket Mode needs no Request URL.
4. **Interactivity:** turn it on. This delivers the button clicks. Socket Mode
   needs no Request URL.
5. Invite the bot to its one channel and note the channel id (`C…`).

The connector makes only outbound connections: to Slack and to aurora-dist.

## Configuration

All configuration comes from the environment. The connector holds only the
Slack credentials. The LLM endpoint, keys and grants are in the manifest.

| Variable | Required | Default | Meaning |
| --- | --- | --- | --- |
| `SLACK_BOT_TOKEN` | yes | | Bot token (`xoxb-…`) |
| `SLACK_APP_TOKEN` | yes | | App-level token (`xapp-…`, `connections:write`) for Socket Mode |
| `SLACK_CHANNEL_ID` | yes | | The one channel to serve |
| `AURORA_MANIFEST` or `AURORA_MANIFEST_FILE` | yes | | The manifest for every process (JSON, or a path) |
| `AURORA_BASE_URL` | | `http://localhost:8080` | The aurora-dist address. A host name other than `localhost` or an IP address must be in aurora-dist's `trusted_hosts`. |
| `SLACK_TRIGGER_KEYWORD` | | `@duty` | A keyword that starts a thread. A mention of the bot always works. |
| `SLACK_BOT_USER_ID` | | from `auth.test` | The bot's user id |
| `SLACK_API_BASE_URL` | | `https://slack.com/api` | For a gateway or for tests |
| `POLL_INTERVAL` | | `2s` | How often a running process is polled |
| `PROCESS_TIMEOUT` | | `15m` | How long to follow one process |
| `HTTP_TIMEOUT` | | `30s` | Timeout of one request to Aurora or Slack |

A duration is a Go duration (`2s`, `10m`) or a number of seconds.

### The manifest

The connector sends the manifest to Aurora unchanged. Start from the minimal
manifest in the [aurora-dist README](https://github.com/aurora-capcompute/aurora-dist).
Set `require_approval: true` on every capability that changes something (for
example a `POST` to an internal endpoint). A Slack message is untrusted input,
so a gated call needs a human. Read-only lookups can stay ungated.

## Run

```sh
export SLACK_BOT_TOKEN=xoxb-...
export SLACK_APP_TOKEN=xapp-...
export SLACK_CHANNEL_ID=C0123456789
export AURORA_MANIFEST_FILE=./duty-manifest.json
go run ./cmd/aurora-slack-connector
```

Then, in the channel:

> **@duty** why are we seeing elevated 500s on checkout?

## Test

```sh
go vet ./...
go test -race ./...
```

The tests stub aurora-dist, the Slack API and the Socket Mode connection with
`httptest` servers. They cover session creation, process start, journal
polling, progress updates, duplicate deliveries, envelope acknowledgement and
reconnection, thread-to-session reattachment, and the full approval flow.

## Limits

- One channel per connector.
- Run one connector per app. Slack spreads the events over all open Socket
  Mode connections of the app.
- Approvals are approve or deny. Other resolutions (such as `completed` with
  data) have no button.
- The dedup set and the worker registry are in memory. The session name and
  Aurora's task state carry continuity across a restart.
