# ue-mcp-forwarder

**A resident forwarding layer for Unreal Editor's MCP** — keeps Unreal MCP capabilities stable and available for AI clients (Trae / Claude / Cursor / ...), even while the Unreal Editor is being opened, closed, and restarted.

> Single-file Rust implementation (~550 lines). The binary is ~650KB with zero runtime dependencies.
> How it works: `AI client (stdio MCP)` ↔ `this forwarder (resident stdio MCP server)` ↔ `Unreal Editor MCP (Streamable HTTP, lives and dies with the editor)`.

## Why

The MCP server built into Unreal Editor (the `ModelContextProtocol` plugin) is bound to the editor process:

- When the editor closes, the MCP entry in your AI client dies with it, taking the whole tool context away.
- After an editor restart, clients usually need a manual reconnect.
- Client-private notifications (e.g. Trae's `notifications/trae/*`) make the editor log errors and can even disturb MCP sessions.

This forwarder absorbs all of that in the middle:

| Scenario | Behavior |
|---|---|
| Editor closed | Forwarder stays resident and returns a structured `editor_offline` error, so the AI agent can tell the user to start the editor |
| Editor started | Heartbeat detects it automatically (default 10s), re-handshakes, and records one probe `tools/list` call |
| Editor restarted (session lost) | Automatic reconnect + fresh `initialize`, transparent to the client |
| Client-private notifications | Intercepted locally (`notifications/trae/*`) — no editor errors, no session churn |
| Parallel tool calls | Request-level concurrent forwarding, no head-of-line blocking |

## Features

- **Zero-config by default**: `127.0.0.1:8000/mcp` + `./logs` + 10s heartbeat — runs with no arguments
- **Zero-parse passthrough**: response payloads never go through a JSON parser; raw bytes stream straight to stdout, forwarding overhead ≈ 0 ms
- **Lightweight heartbeat**: zero traffic while online; one probe every 10s while offline (2s connect timeout)
- **Structured offline errors**: JSON-RPC errors carry `data.status = "editor_offline"`, `editor_required`, `auto_reconnect` — machine-readable for AI agents
- **Bidirectional forwarding log**: NDJSON per day (`logs/forward_YYYYMMDD.ndjson`) with timestamp, direction, method, HTTP status, latency, and payload
- **Oversized-payload spill**: payloads over 200KB are truncated in the main log while the full payload is written to `logs/spill/`; the log record points at the spill file
- **Async logging**: a background thread does all disk I/O — the forwarding path never touches the disk; log files rotate daily
- **Robust SSE handling**: responses return as soon as the matching id arrives (spec-compliant); notifications are answered and closed without reading the body, immune to servers that hold SSE streams open
- **Offline fast-fail**: after a failure, blind reconnects are suppressed for 2s so every request doesn't pay double connect timeouts

## Quick Start

### 1. Unreal side (once)

Enable the **ModelContextProtocol** plugin in the editor (Edit → Plugins → search "Model Context Protocol"), then restart the editor. It listens on `127.0.0.1:8000/mcp` by default (port configurable in plugin settings). Requires UE 5.6+.

### 2. AI client side

Grab a binary from [Releases](../../releases) or [build it yourself](#build), then add to your MCP config:

```json
{
  "mcpServers": {
    "ue": {
      "command": "C:\\path\\to\\ue-mcp-forwarder.exe"
    }
  }
}
```

No arguments required. All options are optional:

| Flag | Default | Description |
|---|---|---|
| `--port <n>` | `8000` | Unreal Editor MCP port |
| `--host <addr>` | `127.0.0.1` | Unreal Editor MCP host |
| `--log-dir <dir>` | `logs` | Forwarding log directory |
| `--heartbeat-secs <n>` | `10` | Heartbeat interval (seconds) |
| `--timeout-secs <n>` | `600` | Per-request total timeout |
| `--log-full` | off | Disable truncation (full payloads in the main log, no spill files) |

### 3. Multiple projects

The forwarder targets an *editor instance*, not a project. Run one editor per project, give each instance a different plugin port (8000, 8001, ...), and add `"args": ["--port", "8001"]` to the client config.

## Forwarding Log

One JSON object per line in `logs/forward_YYYYMMDD.ndjson`:

```json
{"ts":"2026-10-11T02:25:26.580Z","dir":"C->E","method":"tools/list","http":null,"latency_ms":0,"payload":{...}}
```

Payloads over 200KB are truncated and point at the full copy:

```json
{"payload":{"_truncated":true,"bytes":1059421,"spill_file":"spill/20261011T022243599Z_tools_list_0000.json","preview":"..."}}
```

## Offline Error Shape

```json
{
  "code": -32002,
  "message": "UE editor MCP is offline: no running Unreal Editor detected. Start the Unreal Editor (with the ModelContextProtocol plugin enabled) to use MCP operations.",
  "data": {
    "status": "editor_offline",
    "editor_required": true,
    "auto_reconnect": true,
    "heartbeat_secs": 10
  }
}
```

## Build

```bash
git clone https://github.com/YuanBaoSMadLab/ue-mcp-forwarder.git
cd ue-mcp-forwarder
cargo build --release
```

Output: `target/release/ue-mcp-forwarder.exe`. Dependencies are minimal (`serde` / `serde_json` / `ureq`, no TLS feature), built with `opt-level = "z"` + LTO + strip. A `build.bat` is included for Windows convenience.

## Compatibility

- The built-in Unreal MCP (`ModelContextProtocol` plugin, Streamable HTTP) in UE 5.6+
- Any MCP Streamable HTTP server in theory (SSE responses supported)
- Any stdio-based MCP client (Trae, Claude Code, Cursor, ...)

## License

[MIT](LICENSE)
