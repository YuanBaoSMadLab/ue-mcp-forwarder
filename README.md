# ue-mcp-forwarder

**UE 编辑器 MCP 常驻转发层** —— 让 AI 客户端（Trae / Claude / Cursor 等）在 Unreal 编辑器反复开关的情况下，依然拥有稳定可用的 Unreal MCP 能力。

> Rust 单文件实现（约 550 行），编译产物仅 ~650KB，零运行时依赖。
> 原理：AI 客户端 ↔ 本转发器（stdio MCP Server，常驻）↔ UE 编辑器 MCP（Streamable HTTP，随编辑器生死）。

## 为什么需要它

UE 编辑器的 MCP（`ModelContextProtocol` 插件）绑定在编辑器进程上：

- 编辑器一关，AI 客户端里的 MCP 整体失效，工具上下文丢失；
- 编辑器重启后，客户端侧经常需要手动重连；
- 多会话/多实例场景下，客户端私有通知（如 Trae 的 `notifications/trae/*`）会让编辑器报错，甚至扰乱会话。

本转发器把这些问题全部拦在中间层：

| 场景 | 表现 |
|---|---|
| 编辑器关闭 | 转发器常驻，返回明确的 `editor_offline` 警告字段，AI 可直接提示用户"启动编辑器" |
| 编辑器启动 | 心跳自动感应（默认 10s），自动握手 + 一次 `tools/list` 模拟请求记录工具规模 |
| 编辑器重启（会话失效） | 自动重连并重新 initialize，对客户端透明 |
| 客户端私有通知 | 本地拦截（`notifications/trae/*`），不打扰编辑器、不扰动会话 |
| 并发工具调用 | 请求级并发转发，互不阻塞 |

## 特性

- **零配置可用**：默认 `127.0.0.1:8000/mcp` + `./logs` + 10s 心跳，不传参数直接跑
- **报文零解析直通**：响应报文不进 JSON 解析器，原始字符串直通 stdout，转发开销 ≈ 0ms
- **轻度心跳**：在线期间零流量；离线时每 10s 探测一次（连接超时 2s），上线即做模拟请求记录
- **离线警告字段**：JSON-RPC 错误内含 `data.status = "editor_offline"`、`editor_required`、`auto_reconnect`，AI 智能体可程序化判定
- **双向转发记录**：NDJSON 落盘（`logs/forward_YYYYMMDD.ndjson`），含时间戳、方向、方法、HTTP 状态、延迟、完整报文
- **大报文溢出落盘**：超 200K 的报文截断进主日志，同时全量写入 `logs/spill/`，记录内含 `spill_file` 位置可溯源
- **异步日志**：后台线程落盘，关键路径零磁盘 IO；跨天自动切文件
- **SSE 稳健处理**：请求读到匹配响应即返回（MCP 规范允许）；notification 不读 body 立即关连接，免疫"服务器保持 SSE 流打开"的机制
- **离线快速失败**：2 秒内失败过不再盲试重连，避免每个请求都付出双倍连接超时

## 快速开始

### 1. UE 侧（一次性）

编辑器中启用 **ModelContextProtocol** 插件（Edit → Plugins → 搜索 "Model Context Protocol"），重启编辑器。默认监听 `127.0.0.1:8000/mcp`（端口可在插件设置中修改）。

### 2. AI 客户端侧

下载 Release 中的 exe，或在 [构建](#构建) 自行编译，然后在 MCP 配置中添加：

```json
{
  "mcpServers": {
    "ue": {
      "command": "E:\\path\\to\\ue-mcp-forwarder.exe"
    }
  }
}
```

零参数即可运行。所有参数均可选：

| 参数 | 默认值 | 说明 |
|---|---|---|
| `--port <n>` | `8000` | UE 编辑器 MCP 端口 |
| `--host <addr>` | `127.0.0.1` | UE 编辑器 MCP 地址 |
| `--log-dir <dir>` | `logs` | 转发记录目录 |
| `--heartbeat-secs <n>` | `10` | 心跳间隔（秒） |
| `--timeout-secs <n>` | `600` | 单次请求总超时 |
| `--log-full` | 关 | 不截断大报文（全量写主日志，不产生 spill 文件） |

### 3. 多项目

转发器连接的是"编辑器实例"而非项目。一个项目开一个编辑器实例，各实例在插件设置里配不同端口（如 8000、8001），客户端加一段 `"args": ["--port", "8001"]` 即可。

## 转发记录格式

`logs/forward_YYYYMMDD.ndjson`，每行一条：

```json
{"ts":"2026-10-11T02:25:26.580Z","dir":"C->E","method":"tools/list","http":null,"latency_ms":0,"payload":{...}}
```

超 200K 的报文在记录中标记截断并指向全量文件：

```json
{"payload":{"_truncated":true,"bytes":1059421,"spill_file":"spill/20261011T022243599Z_tools_list_0000.json","preview":"..."}}
```

## 编辑器离线时的错误

```json
{
  "code": -32002,
  "message": "UE 编辑器 MCP 服务已下线：未检测到运行中的 Unreal 编辑器。需要启动 Unreal 编辑器（并启用 ModelContextProtocol 插件）才能进行 MCP 操作。",
  "data": {
    "status": "editor_offline",
    "editor_required": true,
    "auto_reconnect": true,
    "heartbeat_secs": 10
  }
}
```

## 构建

```bat
git clone https://github.com/YuanBaoSMadLab/ue-mcp-forwarder.git
cd ue-mcp-forwarder
build.bat          :: 或 cargo build --release
```

产物：`target/release/ue-mcp-forwarder.exe`。依赖仅 `serde` / `serde_json` / `ureq`（无 TLS 特性），`opt-level = "z"` + LTO + strip。

## 兼容性

- Unreal Engine 5.6+ 内置的 Unreal MCP（`ModelContextProtocol` 插件，Streamable HTTP）
- 理论上兼容任何 MCP Streamable HTTP 服务端（含 SSE 响应）
- AI 客户端：任何支持 stdio MCP 的客户端（Trae、Claude Code、Cursor 等）

## License

[MIT](LICENSE)
