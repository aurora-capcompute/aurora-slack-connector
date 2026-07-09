# aurora-slack-connector

**An Aurora agent that lives in a Slack channel.** This is a small HTTP server that
turns one Slack channel into an on‑call **duty bot** backed by a local
[aurora-dist](https://github.com/aurora-capcompute/aurora-dist) runtime. Mention it,
and it opens an Aurora session for the thread, runs your message as an agent, narrates
what it's doing back into the thread, and — when it wants to do something sensitive —
asks you to approve or deny right there with buttons.

> New here? Read [What is this](#what-is-this-in-plain-words), then
> [Set it up locally](#set-it-up-locally). It has **zero non‑stdlib dependencies**.

---

## What is this, in plain words?

You @‑mention the bot in a channel: *"@duty why are we seeing elevated 500s on
checkout?"* The connector:

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
Slack events ──▶ ┌────────────────────────┐ ──localhost /v1──▶ aurora-dist
Slack actions ─▶ │ aurora-slack-connector │ ◀── poll: journal, tasks
     ▲           └────────────────────────┘
     └──── chat.postMessage / update / Approve·Deny buttons (in-thread) ──┘
```

You run [aurora-dist](https://github.com/aurora-capcompute/aurora-dist) as its own
process on the same machine; this connector is a client of it.

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
- **Live progress** — a single status message updated with the syscall timeline.
- **Human‑in‑the‑loop approvals** — Block Kit Approve/Deny buttons; the click is
  signature‑verified and resolved via `POST /v1/tasks/{id}/resolve`, recording who
  decided. The secret resolution token **never travels through Slack** — only
  non‑secret ids ride in the button; the token is fetched from Aurora at click time,
  so the flow survives a connector restart.
- **Robust ingestion** — every inbound request is HMAC‑signature verified (5‑minute
  freshness window), and duplicate deliveries are de‑duped.
- **Health check** — `GET /healthz` returns `ok`.

> **Note:** this uses Slack's **HTTP Events API** (not Socket Mode) and triggers on
> mentions / channel messages — there are **no slash commands** and no
> `SLACK_APP_TOKEN`. It needs a public HTTPS URL Slack can reach.

## Set it up locally

**Prerequisites:** Go 1.26+, a running
[aurora-dist](https://github.com/aurora-capcompute/aurora-dist) on
`http://localhost:8080`, a Slack workspace where you can create an app, and a way to
expose a public HTTPS URL in dev (a tunnel such as ngrok).

### 1. Create a Slack app (from scratch)

1. **OAuth Bot Token Scopes:** `app_mentions:read`, `channels:history` (or
   `groups:history` for a private channel), `chat:write`. Install the app and copy
   the **Bot User OAuth Token** (`xoxb-…`).
2. **Event Subscriptions:** enable; set the Request URL to
   `https://<your-host>/slack/events`; subscribe to the bot events `app_mention` and
   `message.channels` (or `message.groups`). The connector auto‑answers Slack's
   verification handshake.
3. **Interactivity & Shortcuts:** enable; set the Request URL to
   `https://<your-host>/slack/interactions` (this delivers the Approve/Deny clicks).
4. **Signing secret:** copy it from *Basic Information → App Credentials*.
5. Invite the bot to the one channel it should serve and note that channel's ID
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
     "base_url": "https://api.openai.com/v1", "api_key": "sk-...",
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

Set `require_approval: true` on any capability that should need human sign‑off — the
bot will raise an Approve/Deny prompt for it. The manifest must stay within
aurora-dist's capability ceiling or process creation is refused.

### 3. Build, run, and use it

```sh
git clone https://github.com/aurora-capcompute/aurora-slack-connector
cd aurora-slack-connector

export SLACK_BOT_TOKEN=xoxb-...
export SLACK_SIGNING_SECRET=...
export SLACK_CHANNEL_ID=C0123456789
export AURORA_MANIFEST_FILE=./duty-manifest.json
export AURORA_BASE_URL=http://localhost:8080

go run ./cmd/aurora-slack-connector
# or: go build -o aurora-slack-connector ./cmd/aurora-slack-connector && ./aurora-slack-connector
```

It listens on `:3000` by default. Point your tunnel at it so the two Slack Request
URLs resolve. Then, in the channel:

> **@duty** why are we seeing elevated 500s on checkout?

The bot opens a thread, reports each step, and replies with what it found. Keep
replying in the thread to keep digging in the same session.

## Configuration

All configuration is by environment variable. The connector holds **no secrets of
its own** beyond the Slack credentials — the LLM endpoint, key, and capability
grants live in the manifest.

| Variable | Required | Default | Meaning |
| --- | --- | --- | --- |
| `SLACK_BOT_TOKEN` | ✅ | — | Bot token (`xoxb-…`) for Web API calls |
| `SLACK_SIGNING_SECRET` | ✅ | — | Verifies inbound Slack requests |
| `SLACK_CHANNEL_ID` | ✅ | — | The single channel to serve (`C…`) |
| `AURORA_MANIFEST` *or* `AURORA_MANIFEST_FILE` | ✅ | — | The manifest (inline JSON, or a path) applied to every process |
| `AURORA_BASE_URL` | | `http://localhost:8080` | The local aurora-dist address |
| `SLACK_TRIGGER_KEYWORD` | | `@duty` | Keyword that starts a thread (a native @-mention always works too) |
| `SLACK_BOT_USER_ID` | | auto (`auth.test`) | The bot's own user id, for mention‑stripping |
| `SLACK_API_BASE_URL` | | `https://slack.com/api` | Override for an enterprise gateway / testing |
| `ADDR` | | `:3000` | Listen address |
| `EVENTS_PATH` | | `/slack/events` | Path Slack posts events to |
| `INTERACTIONS_PATH` | | `/slack/interactions` | Path Slack posts button clicks to (must differ from `EVENTS_PATH`) |
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
  connector.go    HTTP mux, event ingestion/routing, per-thread worker, process polling
  approvals.go    the interactions endpoint + approval prompt + task resolution
  aurora.go       the aurora-dist /v1 HTTP client
  slack.go        the Slack Web API client + signature verification
  progress.go     renders the syscall journal into a human status timeline
  seen.go         bounded event de-duplication
```

## Development

Pure Go standard library — no dependencies.

```sh
go vet ./...
go test -race ./...
go build ./...
```

The tests stub both aurora-dist and the Slack Web API with `httptest` servers and
drive the connector end to end, including the full approval flow (prompt → signed
button click → task resolution → answer).

## Scope and limitations

- **One channel, one connector.** By design.
- **Events API only** (HTTP) — it needs a public URL. No Socket Mode, no slash
  commands.
- **Approvals are approve/deny.** Richer resolutions Aurora supports aren't exposed
  as buttons. Timer parks resolve themselves and are never prompted.

## Related repos

- [aurora-dist](https://github.com/aurora-capcompute/aurora-dist) — the server this connector talks to
- [aurora-cli](https://github.com/aurora-capcompute/aurora-cli) — a terminal client for the same API
- [aurora-brains](https://github.com/aurora-capcompute/aurora-brains) — the agent programs behind the bot
- [capcompute](https://github.com/aurora-capcompute/capcompute) · [aurora-capcompute](https://github.com/aurora-capcompute/aurora-capcompute) — the kernel and runtime underneath
