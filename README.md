# aurora-slack-connector

**An Aurora agent that lives in a Slack channel.** This is a small service that
turns one Slack channel into an on‑call **duty bot** backed by a local
[aurora-dist](https://github.com/aurora-capcompute/aurora-dist) runtime. Mention it —
or react to a message with a configured emoji — and it opens an Aurora session for
the thread, runs the message as an agent, narrates what it's doing back into the
thread, and — when it wants to do something sensitive — asks you to approve or deny
right there with buttons.

> New here? Read [What is this](#what-is-this-in-plain-words), then
> [Set it up locally](#set-it-up-locally). It connects to Slack over **Socket Mode**,
> so it needs no public URL. Its only dependency beyond the Go standard library is a
> WebSocket client (`github.com/coder/websocket`).

---

## What is this, in plain words?

You @‑mention the bot in a channel: *"@duty why are we seeing elevated 500s on
checkout?"* (or react to an alert with `:eyes:`). The connector:

1. Opens an Aurora **session** for that Slack thread.
2. Runs your message as an Aurora **process** (an agent that can search the web,
   read shared memory, reason).
3. Keeps a status message updated with what it's doing (`🧠 thinking`,
   `🌐 querying the internet …`), then posts the answer.
4. If the agent wants a sensitive action, it **asks first** — Approve / Deny buttons
   appear in the thread, and a human decides.

Follow‑up replies in the same thread run as further processes in the **same
session**, so the investigation shares history — the bot remembers what it already
found.

Its purpose (for now) is a duty bot that **gathers information to help a human
mitigate an incident**. The connector itself is deliberately thin: all the
intelligence, capabilities, and secrets live in Aurora and the manifest you give it.
It does **not** embed the runtime — it's a pure client of aurora-dist's `/v1` API
over localhost.

## Where this fits in the Aurora system

```
              ┌────────────────────────┐ ──localhost /v1──▶ aurora-dist
Slack ◀──wss──▶│ aurora-slack-connector │ ◀── poll: journal, tasks
(Socket Mode)  └────────────────────────┘
     └── chat.postMessage / update / reactions / Approve·Deny buttons ──┘
```

The connector opens an outbound **Socket Mode** WebSocket to Slack (no public URL,
no request signing) and talks to
[aurora-dist](https://github.com/aurora-capcompute/aurora-dist) — which you run as
its own process on the same machine — over its localhost `/v1` API.

| Slack | Aurora | Notes |
| --- | --- | --- |
| a thread | a **session** (`slack:<thread_ts>`) | one thread ⇄ one session |
| a message | a **process** | the message text is the input |
| the bot's reply | the process's answer, polled to completion | posted into the thread |
| "what is it doing?" | the process **journal** | each syscall narrated as a status line |
| an approval prompt | a pending **task** | Approve/Deny buttons in the thread |

## Features

- **Thread ⇄ session** — reattaches to the existing session by name across
  restarts; never starts a second session for a thread.
- **Shared history** — message *n* sees the conversation of messages *1 … n‑1*.
- **Serialized per thread** — one active process per thread (follow‑ups queue);
  different threads run concurrently.
- **Triggers** — an @‑mention, the configured keyword, or the configured trigger
  emoji added to a message. The bot acknowledges a message it works on with
  reactions (👀 → ✅/❌).
- **Live progress** — a single status message updated with the syscall timeline.
- **Human‑in‑the‑loop approvals** — Block Kit Approve/Deny buttons resolved via
  `POST /v1/tasks/{id}/resolve`, recording who decided. The secret resolution token
  **never travels through Slack** — only non‑secret ids ride in the button; the
  token is fetched from Aurora at click time, so the flow survives a connector
  restart.
- **Health check** — `GET /healthz` returns `ok`.

> **Note:** this uses Slack's **Socket Mode** (an outbound WebSocket), so it needs a
> `SLACK_APP_TOKEN` and **no public URL**. It triggers on mentions, the keyword, and
> the trigger reaction — there are no slash commands.

## Set it up locally

**Prerequisites:** Go 1.26+, a running
[aurora-dist](https://github.com/aurora-capcompute/aurora-dist) on
`http://localhost:8080`, and a Slack workspace where you can create an app. Because
Socket Mode is outbound, you need only egress to Slack — no tunnel or public URL.

### 1. Create a Slack app (from scratch)

The repository's [`manifest.json`](./manifest.json) already describes this app —
create the app *From an app manifest* and paste it, or configure by hand:

1. **Socket Mode:** enable it (*Settings → Socket Mode*). This removes the need for a
   public Request URL.
2. **App‑level token:** create one (*Basic Information → App‑Level Tokens*) with the
   `connections:write` scope and copy it (`xapp-…`).
3. **OAuth Bot Token Scopes:** `app_mentions:read`, `channels:history`,
   `groups:history`, `chat:write`, `reactions:read`, `reactions:write`. Install the
   app and copy the **Bot User OAuth Token** (`xoxb-…`).
4. **Event Subscriptions:** enable and subscribe to the bot events `app_mention`,
   `message.channels`, `message.groups`, `reaction_added`, `reaction_removed`. No
   Request URL is needed in Socket Mode.
5. **Interactivity & Shortcuts:** enable it (no Request URL in Socket Mode) so the
   Approve/Deny clicks are delivered.
6. Invite the bot to the one channel it should serve and note that channel's ID
   (`C…`).

### 2. Write a manifest

The manifest is Aurora's grant set for every process — it names the LLM driver and
the capabilities the bot may use, and it's where the model endpoint and key live. A
starting point for an information‑gathering duty bot:

```json
{
  "version": 4,
  "syscalls": [
    {"syscall": "core.openaiApi", "hidden": true,
     "base_url": "https://api.openai.com/v1", "api_key": {"secret": "OPENAI_KEY"},
     "default_model": "gpt-4o",
     "capabilities": [{"operation": "chat", "require_approval": false}]},
    {"syscall": "core.internet",
     "capabilities": [{"methods": ["GET"], "domain": "status.example.com"}]},
    {"syscall": "core.memory",
     "capabilities": [{"operation": "get"}, {"operation": "put"}]},
    {"syscall": "sys.timer"}
  ]
}
```

The `api_key` is a **secret reference** — the connector forwards the manifest to
aurora-dist verbatim, and aurora-dist resolves the value from its own environment, so
the key never sits in this file. Set it where aurora-dist runs:

```sh
export AURORA_SECRET_OPENAI_KEY=sk-...   # on the aurora-dist process, not the connector
```

Set `require_approval: true` on any capability that should need human sign‑off — the
bot will raise an Approve/Deny prompt for it. The manifest must stay within
aurora-dist's capability ceiling or process creation is refused.

### 3. Build, run, and use it

```sh
git clone https://github.com/aurora-capcompute/aurora-slack-connector
cd aurora-slack-connector

export SLACK_BOT_TOKEN=xoxb-...
export SLACK_APP_TOKEN=xapp-...
export SLACK_CHANNEL_ID=C0123456789
export AURORA_MANIFEST_FILE=./duty-manifest.json
export AURORA_BASE_URL=http://localhost:8080

go run ./cmd/aurora-slack-connector
# or: go build -o aurora-slack-connector ./cmd/aurora-slack-connector && ./aurora-slack-connector
```

It connects to Slack over Socket Mode on startup (and serves `/healthz` on `:3000`).
Then, in the channel:

> **@duty** why are we seeing elevated 500s on checkout?

The bot opens a thread, reports each step, and replies with what it found. Keep
replying in the thread to keep digging in the same session — or react to an alert
with `:eyes:` to have it investigate that message.

## Reactions

Two reaction behaviours, both in the channel the connector serves:

- **React to investigate.** Adding the configured trigger emoji
  (`SLACK_TRIGGER_REACTION`, default `:eyes:`) to any message tells the bot to read
  that message and investigate its text — the "react to an alert and let the bot
  gather data" flow. The investigation runs in a thread rooted at the reacted
  message, exactly as if it had been mentioned. Only the first trigger reaction on a
  message starts an investigation; a reaction on a bot/system message, or on a thread
  reply the bot can't read, is ignored.
- **Acknowledgement.** For every message the bot works on it adds 👀 when it starts
  and swaps it for ✅ on success or ❌ on any non‑success outcome — a glanceable
  state next to the status message.

Both need the `reactions:read` / `reactions:write` scopes and the `reaction_added` /
`reaction_removed` event subscriptions.

## Configuration

All configuration is by environment variable. The connector holds **no secrets of
its own** beyond the Slack credentials — the LLM endpoint, key, and capability
grants live in the manifest.

| Variable | Required | Default | Meaning |
| --- | --- | --- | --- |
| `SLACK_BOT_TOKEN` | ✅ | — | Bot token (`xoxb-…`) for Web API calls |
| `SLACK_APP_TOKEN` | ✅ | — | App‑level token (`xapp-…`, scope `connections:write`) that opens the Socket Mode connection |
| `SLACK_CHANNEL_ID` | ✅ | — | The single channel to serve (`C…`) |
| `AURORA_MANIFEST` *or* `AURORA_MANIFEST_FILE` | ✅ | — | The manifest (inline JSON, or a path) applied to every process |
| `AURORA_BASE_URL` | | `http://localhost:8080` | The local aurora-dist address |
| `SLACK_TRIGGER_KEYWORD` | | `@duty` | Keyword that starts a thread (a native @-mention always works too) |
| `SLACK_TRIGGER_REACTION` | | `eyes` | Emoji that starts an investigation when added to a message (empty disables reaction triggers) |
| `SLACK_BOT_USER_ID` | | auto (`auth.test`) | The bot's own user id, for mention‑stripping |
| `SLACK_API_BASE_URL` | | `https://slack.com/api` | Override for an enterprise gateway / testing |
| `ADDR` | | `:3000` | Listen address for the `/healthz` liveness endpoint (Socket Mode is outbound; no events are served here) |
| `POLL_INTERVAL` | | `2s` | How often a running process is polled |
| `PROCESS_TIMEOUT` | | `15m` | How long to actively report on one process before backing off |
| `HTTP_TIMEOUT` | | `30s` | Per‑request timeout for aurora/Slack calls |

Durations accept a Go duration string (`2s`, `500ms`, `10m`) or a bare integer
meaning seconds.

## Project layout

```
cmd/aurora-slack-connector/main.go   the binary
internal/connector/
  config.go       env parsing + validation
  connector.go    event ingestion/routing, per-thread worker, process polling, reactions
  socket.go       the Socket Mode transport: open, read, acknowledge, reconnect
  approvals.go    the interaction payload handler + approval prompt + task resolution
  aurora.go       the aurora-dist /v1 HTTP client
  slack.go        the Slack Web API client (messages, reactions, history)
  progress.go     renders the syscall journal into a human status timeline
  seen.go         bounded event de-duplication
```

## Development

Go standard library plus a single WebSocket dependency
(`github.com/coder/websocket`) for Socket Mode.

```sh
go vet ./...
go test -race ./...
go build ./...
```

The tests stub aurora-dist, the Slack Web API, and the Socket Mode WebSocket with
`httptest` servers and drive the connector end to end — including the full approval
flow (prompt → button click → task resolution → answer) and the reaction triggers.

## Scope and limitations

- **One channel, one connector.** By design.
- **Socket Mode only** — an outbound WebSocket; it needs egress to Slack but no
  public URL, tunnel, or slash commands.
- **Approvals are approve/deny.** Richer resolutions Aurora supports aren't exposed
  as buttons. Timer parks resolve themselves and are never prompted.

## Related repos

- [aurora-dist](https://github.com/aurora-capcompute/aurora-dist) — the server this connector talks to
- [aurora-cli](https://github.com/aurora-capcompute/aurora-cli) — a terminal client for the same API
- [aurora-brains](https://github.com/aurora-capcompute/aurora-brains) — the agent programs behind the bot
- [capcompute](https://github.com/aurora-capcompute/capcompute) · [aurora-capcompute](https://github.com/aurora-capcompute/aurora-capcompute) — the kernel and runtime underneath
