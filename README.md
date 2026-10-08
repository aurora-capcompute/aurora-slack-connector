# aurora-slack-connector

An HTTP server that turns one Slack channel into a duty bot backed by an
[aurora-dist](https://github.com/aurora-capcompute/aurora-dist) server. Mention
the bot, and it opens an Aurora session for the thread, runs your message as a
process, and reports each syscall in the thread while it works. Later messages
in the thread run as more processes in the same session.

The bot gathers information to help a human handle an incident. When it needs a
sensitive action, it asks first: a human approves or denies in the thread.

```
Slack events  ──▶ ┌────────────────────────┐ ── /v1 ──▶ aurora-dist
Slack actions ──▶ │ aurora-slack-connector │ ◀── poll: journal, tasks
     ▲            └────────────────────────┘
     └── chat.postMessage, update, Approve/Deny buttons (in the thread)
```

The connector is a client of aurora-dist's `/v1` API. It does not embed the
runtime, and it uses only the Go standard library. The agent, its capabilities
and its secrets live in Aurora and in the manifest you give it.

## How Slack maps onto Aurora

| Slack | Aurora |
| --- | --- |
| a thread | a session named `slack:<thread_ts>`, with tags `source`, `channel`, `thread_ts` |
| a message in the thread | a process. The message text is its input. |
| the bot's reply | the process's answer |
| "what is it doing?" | the process's journal. Each syscall becomes a status line. |
| an approval prompt | a pending task, resolved with `POST /v1/tasks/{id}/resolve` |

- **Shared history.** Aurora gives each new process its session's history and
  adds each finished process's input and answer to it. So message *n* sees
  messages *1 … n-1*, unless the manifest sets `"history": false`. The connector
  finds a thread's session by name, also after a restart.
- **One process per thread at a time.** A session runs one process at a time,
  so the connector queues later messages of a thread. Different threads run in
  parallel.
- **Progress by polling.** While a process runs, the connector polls Aurora and
  keeps one status message up to date (for example `🧠 thinking`,
  `🌐 querying the internet …`, where `…` marks the call in progress). Then it
  posts the answer as a new message.

## Approvals

A grant with `require_approval: true` makes Aurora park the process on a task
(`waiting_for_task`). The connector then:

1. Posts a prompt with the task's summary and arguments, and **Approve** /
   **Deny** buttons. The status line shows `⏸️ waiting for your approval`, and
   the process timeout waits.
2. On a click, verifies the Slack signature, reads the task again from the
   session (its state and `resolution_token`), and resolves it with the
   decision. The clicking user is the `actor` in the audit trail.
3. Rewrites the prompt to `✅ Approved by @user` or `🚫 Denied by @user` and
   removes the buttons. Aurora resumes the process.

The resolution token never goes through Slack. The button carries only the
task and session ids, and the connector reads the token from Aurora at click
time. So the flow survives a connector restart. Timers (`sys.timer`) never
raise a prompt: the runtime fires them. Any channel member can approve or deny.

## Slack app setup

1. **Bot token scopes:** `app_mentions:read`, `channels:history` (or
   `groups:history` for a private channel), `chat:write`. Install the app and
   copy the bot token (`xoxb-…`).
2. **Event Subscriptions:** set the Request URL to
   `https://<host>/slack/events`, and subscribe to `app_mention` and
   `message.channels` (or `message.groups`). The connector answers Slack's
   verification request.
3. **Interactivity:** set the Request URL to `https://<host>/slack/interactions`.
   This delivers the button clicks.
4. **Signing secret:** copy it from *Basic Information*. The connector verifies
   every request with it.
5. Invite the bot to its one channel and note the channel id (`C…`).

Slack must reach the connector at a public HTTPS URL (an ingress, or a tunnel in
development).

## Configuration

All configuration comes from the environment. The connector holds only the
Slack credentials. The LLM endpoint, keys and grants are in the manifest.

| Variable | Required | Default | Meaning |
| --- | --- | --- | --- |
| `SLACK_BOT_TOKEN` | yes | | Bot token (`xoxb-…`) |
| `SLACK_SIGNING_SECRET` | yes | | Verifies requests from Slack |
| `SLACK_CHANNEL_ID` | yes | | The one channel to serve |
| `AURORA_MANIFEST` or `AURORA_MANIFEST_FILE` | yes | | The manifest for every process (JSON, or a path) |
| `AURORA_BASE_URL` | | `http://localhost:8080` | The aurora-dist address |
| `SLACK_TRIGGER_KEYWORD` | | `@duty` | A keyword that starts a thread. A mention of the bot always works. |
| `SLACK_BOT_USER_ID` | | from `auth.test` | The bot's user id |
| `SLACK_API_BASE_URL` | | `https://slack.com/api` | For a gateway or for tests |
| `ADDR` | | `:3000` | Listen address |
| `EVENTS_PATH` | | `/slack/events` | Events path |
| `INTERACTIONS_PATH` | | `/slack/interactions` | Button-click path (must differ from `EVENTS_PATH`) |
| `POLL_INTERVAL` | | `2s` | How often a running process is polled |
| `PROCESS_TIMEOUT` | | `15m` | How long to follow one process |
| `HTTP_TIMEOUT` | | `30s` | Timeout of one request to Aurora or Slack |

A duration is a Go duration (`2s`, `10m`) or a number of seconds.

### The manifest

The connector sends the manifest to Aurora unchanged. A start for an
information-gathering bot:

```json
{
  "version": 4,
  "program": "agent",
  "syscalls": [
    {
      "syscall": "core.openaiApi",
      "hidden": true,
      "base_url": "https://api.openai.com/v1",
      "api_key": {"secret": "OPENAI_KEY"},
      "default_model": "gpt-4o",
      "capabilities": [{"operation": "chat", "require_approval": false}]
    },
    {
      "syscall": "core.internet",
      "capabilities": [{"methods": ["GET"], "domain": "status.example.com"}]
    },
    {"syscall": "core.scratch", "capabilities": [{"operation": "get"}, {"operation": "put"}]},
    {"syscall": "sys.timer"}
  ]
}
```

`api_key` names a secret that aurora-dist resolves from
`AURORA_SECRET_OPENAI_KEY`. Aurora refuses an inline key. Set
`require_approval: true` on every capability that changes something (for
example a `POST` to an internal endpoint). A Slack message is untrusted input,
so a gated call needs a human. Read-only lookups can stay ungated.

## Run

```sh
export SLACK_BOT_TOKEN=xoxb-...
export SLACK_SIGNING_SECRET=...
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

The tests stub aurora-dist and the Slack API with `httptest` servers. They
cover session creation, process start, journal polling, progress updates,
duplicate deliveries, signature checks, thread-to-session reattachment, and the
full approval flow.

## Limits

- One channel per connector.
- Events API only, so the connector needs a public URL.
- Approvals are approve or deny. Other resolutions (such as `completed` with
  data) have no button.
- The dedup set and the worker registry are in memory. The session name and
  Aurora's task state carry continuity across a restart.
