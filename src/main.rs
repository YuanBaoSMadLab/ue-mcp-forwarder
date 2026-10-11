// ue-mcp-forwarder —— UE 编辑器 MCP 常驻转发层
//
// 职责（只做这三件事）：
//   1. 常驻：以 stdio MCP Server 形态供 AI 客户端(Trae 等)拉起，进程不随编辑器生死
//   2. 转发：把 JSON-RPC 透明转发到 UE 编辑器 MCP(Streamable HTTP, 默认 127.0.0.1:8000/mcp)
//   3. 记录：双向全部落盘 NDJSON 转发记录
//
// 行为约定：
//   - 编辑器在线：全量透传（不解析不改写工具目录，保留全部 MCP 能力）
//   - 编辑器离线/会话失效：自动重连并重新 initialize；连不上则返回干净 JSON-RPC 错误，进程不退出
//   - stdout 只输出 JSON-RPC 消息；诊断信息全部走 stderr
//
// 性能设计（报文零解析直通管道）：
//   - 大报文不进 serde：响应报文以原始字符串直通 stdout，仅 SSE 分帧需要轻量解析
//   - 异步日志：后台线程落盘，关键路径零磁盘 IO；超 200K 报文截断进主日志并全量落盘
//     spill/ 目录，记录内含 spill_file 位置（--log-full 保留全量不截断）
//   - 请求级并发：每条消息一个工作线程（initialize 例外，内联处理保证会话时序）
//   - SSE 提前返回：读到与请求 id 匹配的响应即断开读取，不等服务器关闭流
//   - 连接复用：ureq Agent 内置连接池；离线快速失败：2 秒内失败过不再盲试

use serde_json::{json, Value};
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

// ---------- 参数 ----------

struct Args {
    host: String,
    port: u16,
    log_dir: PathBuf,
    timeout_secs: u64,
    log_full: bool,
    heartbeat_secs: u64,
}

fn parse_args() -> Args {
    let mut a = Args {
        host: "127.0.0.1".into(),
        port: 8000,
        log_dir: PathBuf::from("logs"),
        timeout_secs: 600,
        log_full: false,
        heartbeat_secs: 10,
    };
    let mut it = std::env::args().skip(1);
    while let Some(k) = it.next() {
        let mut val = || it.next().unwrap_or_default();
        match k.as_str() {
            "--host" => a.host = val(),
            "--port" => a.port = val().parse().unwrap_or(a.port),
            "--log-dir" => a.log_dir = PathBuf::from(val()),
            "--timeout-secs" => a.timeout_secs = val().parse().unwrap_or(a.timeout_secs),
            "--log-full" => a.log_full = true,
            "--heartbeat-secs" => a.heartbeat_secs = val().parse().unwrap_or(a.heartbeat_secs),
            _ => {}
        }
    }
    a
}

// ---------- UTC 时间（零依赖，ISO8601 毫秒） ----------

fn split_utc(secs: u64) -> (i64, u32, u32, u32, u32, u32) {
    let days = (secs / 86400) as i64;
    let rem = secs % 86400;
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    let y = if m <= 2 { y + 1 } else { y };
    (
        y,
        m,
        d,
        (rem / 3600) as u32,
        ((rem % 3600) / 60) as u32,
        (rem % 60) as u32,
    )
}

fn iso_now() -> String {
    let n = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default();
    let (y, mo, d, h, mi, s) = split_utc(n.as_secs());
    format!(
        "{y:04}-{mo:02}-{d:02}T{h:02}:{mi:02}:{s:02}.{:03}Z",
        n.subsec_millis()
    )
}

fn today() -> String {
    let n = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default();
    let (y, mo, d, _, _, _) = split_utc(n.as_secs());
    format!("{y:04}{mo:02}{d:02}")
}

// ---------- 异步转发记录 ----------

const LOG_TRUNCATE: usize = 200 * 1024; // 单条 payload 上限 200K，超出截断并溢出落盘（--log-full 关闭截断）
const SPILL_PREVIEW: usize = 16 * 1024; // 记录里保留的报文预览长度

// 客户端私有通知前缀：编辑器不认识，转发只会产生错误日志与无谓的会话扰动，本地直接拦截
const CLIENT_PRIVATE_NOTIFICATIONS: &[&str] = &["notifications/trae/"];

// 后台日志线程：跨天自动换文件，逐行 flush
fn spawn_logger(dir: PathBuf) -> mpsc::Sender<String> {
    let _ = fs::create_dir_all(&dir);
    let (tx, rx) = mpsc::channel::<String>();
    std::thread::spawn(move || {
        let mut cur: Option<(String, File)> = None;
        for line in rx {
            let t = today();
            if cur.as_ref().map(|(d, _)| d.as_str()) != Some(t.as_str()) {
                let path = dir.join(format!("forward_{t}.ndjson"));
                match OpenOptions::new().create(true).append(true).open(&path) {
                    Ok(f) => cur = Some((t, f)),
                    Err(e) => {
                        eprintln!("[forwarder] 日志文件打开失败 {path:?}: {e}");
                        continue;
                    }
                }
            }
            if let Some((_, f)) = cur.as_mut() {
                let _ = writeln!(f, "{line}");
                let _ = f.flush();
            }
        }
    });
    tx
}

struct Upstream {
    url: String,
    agent: ureq::Agent,
    session: Mutex<Option<String>>,
    last_fail: Mutex<Option<Instant>>,
    online: AtomicBool, // 编辑器 MCP 是否可达（心跳与转发共同维护）
    log: mpsc::Sender<String>,
    spill_dir: PathBuf,     // 超限报文全量落盘目录 (<log_dir>/spill)
    spill_seq: AtomicUsize, // 溢出文件序号，保证唯一
    log_full: bool,
    heartbeat_secs: u64,
}

impl Upstream {
    fn new(host: &str, port: u16, timeout_secs: u64, log_dir: PathBuf, log_full: bool, heartbeat_secs: u64) -> Self {
        let _ = fs::create_dir_all(&log_dir);
        let spill_dir = log_dir.join("spill");
        let _ = fs::create_dir_all(&spill_dir);
        let agent = ureq::AgentBuilder::new()
            .timeout_connect(Duration::from_secs(2))
            .timeout(Duration::from_secs(timeout_secs))
            .build();
        Upstream {
            url: format!("http://{host}:{port}/mcp"),
            agent,
            session: Mutex::new(None),
            last_fail: Mutex::new(None),
            online: AtomicBool::new(false),
            log: spawn_logger(log_dir),
            spill_dir,
            spill_seq: AtomicUsize::new(0),
            log_full,
            heartbeat_secs,
        }
    }

    // 一条转发记录。payload_line 必须是合法 JSON（来自报文原文，零再序列化）
    // 超过 200K：截断进主日志，同时把完整报文全量落盘到 spill/ 并在记录中给出位置
    fn rec(&self, dir: &str, method: &str, http: Option<u16>, latency_ms: u128, payload_line: &str) {
        let ts = iso_now();
        let m = serde_json::to_string(method).unwrap_or_else(|_| "\"?\"".into());
        let h = http.map(|c| c.to_string()).unwrap_or_else(|| "null".into());
        let payload: String = if !self.log_full && payload_line.len() > LOG_TRUNCATE {
            let seq = self.spill_seq.fetch_add(1, Ordering::SeqCst);
            let safe: String = method
                .chars()
                .map(|c| if c.is_ascii_alphanumeric() || c == '_' { c } else { '_' })
                .collect();
            let fname = format!("{}_{}_{seq:04}.json", ts.replace(['-', ':', '.'], ""), safe);
            let full = self.spill_dir.join(&fname);
            let preview =
                serde_json::to_string(&payload_line.get(..SPILL_PREVIEW).unwrap_or("")).unwrap_or_default();
            match fs::write(&full, payload_line) {
                Ok(_) => format!(
                    r#"{{"_truncated":true,"bytes":{},"spill_file":"spill/{fname}","preview":{preview}}}"#,
                    payload_line.len()
                ),
                Err(e) => {
                    let es = serde_json::to_string(&e.to_string()).unwrap_or_default();
                    eprintln!("[forwarder] 溢出报文落盘失败 {:?}: {e}", full);
                    format!(
                        r#"{{"_truncated":true,"bytes":{},"spill_file":null,"error":{es},"preview":{preview}}}"#,
                        payload_line.len()
                    )
                }
            }
        } else {
            payload_line.to_string()
        };
        let line = format!(
            r#"{{"ts":"{ts}","dir":"{dir}","method":{m},"http":{h},"latency_ms":{latency_ms},"payload":{payload}}}"#
        );
        let _ = self.log.send(line);
    }

    // 单次 POST。返回 (状态码, 响应头 session, 未消费的响应)
    fn post_raw(&self, body: &str, with_session: bool) -> Result<(u16, Option<String>, ureq::Response), String> {
        let mut req = self
            .agent
            .post(&self.url)
            .set("Content-Type", "application/json")
            .set("Accept", "application/json, text/event-stream");
        if with_session {
            let sid = self.session.lock().unwrap().clone();
            if let Some(sid) = sid {
                req = req.set("Mcp-Session-Id", &sid);
            }
        }
        match req.send_string(body) {
            Ok(resp) => Ok((resp.status(), resp.header("mcp-session-id").map(|s| s.to_string()), resp)),
            Err(ureq::Error::Status(_code, resp)) => {
                Ok((resp.status(), resp.header("mcp-session-id").map(|s| s.to_string()), resp))
            }
            Err(e) => Err(format!("transport: {e}")),
        }
    }

    // 自动 initialize 握手（断线重连时代理主动发起）。持锁执行，避免并发握手风暴
    fn ensure_session(&self) -> Result<(), String> {
        let mut guard = self.session.lock().unwrap();
        if guard.is_some() {
            return Ok(());
        }
        let init = json!({
            "jsonrpc": "2.0", "id": 0, "method": "initialize",
            "params": {
                "protocolVersion": "2025-06-18",
                "capabilities": {},
                "clientInfo": {"name": "ue-mcp-forwarder", "version": "0.3.0"}
            }
        })
        .to_string();
        let t = Instant::now();
        let (status, sid, resp) = self.post_raw(&init, false).map_err(|e| {
            *self.last_fail.lock().unwrap() = Some(Instant::now());
            e
        })?;
        let body = resp.into_string().map_err(|e| format!("read body: {e}"))?;
        self.rec("E->C(代理握手)", "initialize", Some(status), t.elapsed().as_millis(), &body);
        if !(200..300).contains(&status) {
            return Err(format!("initialize HTTP {status}"));
        }
        match sid {
            Some(sid) => {
                *guard = Some(sid.clone());
                drop(guard); // 先释放会话锁，post_raw 内部还要取锁
                eprintln!("[forwarder] 已建立编辑器会话: {sid}");
                let note = json!({"jsonrpc":"2.0","method":"notifications/initialized"}).to_string();
                let _ = self.post_raw(&note, true);
                Ok(())
            }
            None => Err("编辑器未返回 Mcp-Session-Id".into()),
        }
    }

    // 会话失效后的重置 + 重新握手
    fn reset_and_handshake(&self) -> Result<(), String> {
        *self.session.lock().unwrap() = None;
        eprintln!("[forwarder] 会话失效，重新握手…");
        self.ensure_session()
    }

    // 心跳探测：仅离线时动作——握手 + 一次 tools/list 模拟请求（记录工具规模，验证端到端）
    // 在线时零流量（轻度）。返回 Some(工具数) 表示本次探测成功上线
    fn probe(&self) -> Option<usize> {
        if self.online.load(Ordering::SeqCst) {
            return None; // 已在线，不打扰
        }
        if self.ensure_session().is_err() {
            return None; // 仍离线，静默等下一轮
        }
        // 模拟请求 tools/list：验证端到端并记录
        let body = json!({"jsonrpc":"2.0","id":0,"method":"tools/list"}).to_string();
        let t = Instant::now();
        match self.post_raw(&body, true) {
            Ok((status, _, resp)) => {
                let raw = resp.into_string().unwrap_or_default();
                self.rec("C->E", "tools/list(心跳模拟)", Some(status), 0, &body);
                self.rec("E->C", "tools/list(心跳模拟)", Some(status), t.elapsed().as_millis(), &raw);
                let n = serde_json::from_str::<Value>(&raw).ok()
                    .and_then(|v| v.pointer("/result/tools").and_then(|t| t.as_array()).map(|a| a.len()));
                self.online.store(true, Ordering::SeqCst);
                match n {
                    Some(n) => {
                        eprintln!("[forwarder] 编辑器已上线: {} 个工具可转发 ({:.0}ms)", n, t.elapsed().as_millis());
                        self.rec("SYS", "heartbeat", None, 0, &json!({"event":"editor_online","tools":n,"latency_ms":t.elapsed().as_millis()}).to_string());
                    }
                    None => {
                        eprintln!("[forwarder] 编辑器已上线: 转发就绪 ({:.0}ms)", t.elapsed().as_millis());
                        self.rec("SYS", "heartbeat", None, 0, &json!({"event":"editor_online","latency_ms":t.elapsed().as_millis()}).to_string());
                    }
                }
                n
            }
            Err(_) => {
                *self.session.lock().unwrap() = None;
                None
            }
        }
    }

    // 转发一条客户端消息；返回要写回 stdout 的原始 JSON-RPC 报文（零解析直通）
    fn forward(&self, msg: &Value, body: &str) -> Vec<String> {
        let method = msg.get("method").and_then(|m| m.as_str()).unwrap_or("(response)").to_string();
        let expect_id = msg.get("id").cloned();
        // notification（无 id）无响应语义：拿到状态码即关闭连接、不读 body，
        // 彻底规避"服务器保持 SSE 流打开不关"的机制导致线程挂死
        let is_notification = expect_id.is_none();

        // 客户端私有通知（如 notifications/trae/session_stop）：编辑器不认识，
        // 本地拦截不转发，避免编辑器报 unknown method + 400 引发会话重置
        if is_notification && CLIENT_PRIVATE_NOTIFICATIONS.iter().any(|p| method.starts_with(p)) {
            self.rec("C->E", &method, None, 0, body);
            self.rec("E->C", &method, None, 0, &json!({"dropped": "客户端私有通知, 不转发"}).to_string());
            return vec![];
        }

        // 客户端发起新 initialize：视为重置会话
        if method == "initialize" {
            *self.session.lock().unwrap() = None;
        }

        let mut attempt = 0;
        loop {
            if let Err(e) = self.ensure_session() {
                self.online.store(false, Ordering::SeqCst);
                self.rec("C->E", &method, None, 0, body);
                self.rec("E->C", &method, None, 0, &json!({"offline": e}).to_string());
                return vec![editor_offline_error(msg, &self.url, &e, self.heartbeat_secs)];
            }
            self.rec("C->E", &method, None, 0, body);

            let t = Instant::now();
            match self.post_raw(body, true) {
                Ok((status, _, resp)) => {
                    // 通知：任何 HTTP 状态都直接终结（不重试、不重置会话）——
                    // 编辑器对未知通知返回 400 属正常拒绝，与"会话失效"无关
                    if is_notification {
                        drop(resp);
                        let ok = (200..300).contains(&status);
                        if ok {
                            self.online.store(true, Ordering::SeqCst);
                        }
                        self.rec("E->C", &method, Some(status), t.elapsed().as_millis(),
                            &json!({"notification_accepted": ok}).to_string());
                        return vec![];
                    }
                    if (200..300).contains(&status) {
                        self.online.store(true, Ordering::SeqCst);
                        let replies = parse_reply_stream(resp, &expect_id);
                        for r in &replies {
                            self.rec("E->C", &method, Some(status), t.elapsed().as_millis(), r);
                        }
                        return replies;
                    }
                    let err_body = resp.into_string().unwrap_or_default();
                    self.rec("E->C", &method, Some(status), t.elapsed().as_millis(), &err_body);
                    // 会话失效（编辑器重启过）→ 清会话重试一次
                    if (status == 404 || status == 400) && attempt == 0 {
                        attempt += 1;
                        if self.reset_and_handshake().is_ok() {
                            continue;
                        }
                    }
                    return vec![rpc_error_str(msg, -32000, &format!("编辑器返回 HTTP {status}: {err_body}"), json!({"status":"editor_error","http":status}))];
                }
                Err(e) => {
                    // 离线快速失败：若此前 2 秒内刚失败过，跳过重试
                    let was_recent = {
                        let mut lf = self.last_fail.lock().unwrap();
                        let recent = lf.map(|f| f.elapsed() < Duration::from_secs(2)).unwrap_or(false);
                        *lf = Some(Instant::now());
                        recent
                    };
                    self.rec("E->C", &method, None, t.elapsed().as_millis(), &json!({"transport_error": e}).to_string());
                    *self.session.lock().unwrap() = None;
                    if attempt == 0 && !was_recent {
                        attempt += 1;
                        continue; // 首次失败重试一次，覆盖编辑器刚好在重启的窗口
                    }
                    self.online.store(false, Ordering::SeqCst);
                    return vec![editor_offline_error(msg, &self.url, &e, self.heartbeat_secs)];
                }
            }
        }
    }
}

// 从响应提取 JSON-RPC 报文原文列表。
// 非 SSE：整包直通，零解析；SSE：逐行流式读，读到与 expect_id 匹配的响应立即返回
fn parse_reply_stream(resp: ureq::Response, expect_id: &Option<Value>) -> Vec<String> {
    let ctype = resp.header("content-type").unwrap_or("");
    if !ctype.contains("text/event-stream") {
        let mut body = match resp.into_string() {
            Ok(b) => b,
            Err(_) => return vec![],
        };
        let trimmed = body.trim_start();
        if trimmed.starts_with('{') || trimmed.starts_with('[') {
            // JSON 规范禁止字符串内出现裸换行，报文里的 \r/\n 必为结构性空白，压平不影响语义
            if body.contains('\r') || body.contains('\n') {
                body = body.replace(['\r', '\n'], " ");
            }
            return vec![body]; // 纯 JSON 整包直通
        }
        // 极少数情况：SSE 体但 content-type 缺失 → 走字符串分帧
        return sse_parse_str(&body, expect_id);
    }

    // 流式 SSE
    let reader = BufReader::new(resp.into_reader());
    let mut out = Vec::new();
    let mut cur: Vec<String> = Vec::new();
    for line in reader.lines() {
        let line = match line {
            Ok(l) => l,
            Err(_) => break, // 流中断：以已收到的消息为准
        };
        if line.is_empty() {
            if flush_event(&mut cur, &mut out, expect_id) {
                return out; // 已拿到匹配响应
            }
        } else if let Some(rest) = line.strip_prefix("data:") {
            cur.push(rest.strip_prefix(' ').unwrap_or(rest).to_string());
        }
    }
    flush_event(&mut cur, &mut out, expect_id);
    out
}

// 字符串形式的 SSE 分帧（content-type 缺失时的兜底）
fn sse_parse_str(body: &str, expect_id: &Option<Value>) -> Vec<String> {
    let mut out = Vec::new();
    let mut cur: Vec<String> = Vec::new();
    let mut hit = false;
    for line in body.lines() {
        if line.is_empty() {
            if flush_event(&mut cur, &mut out, expect_id) {
                hit = true;
                break;
            }
        } else if let Some(rest) = line.strip_prefix("data:") {
            cur.push(rest.strip_prefix(' ').unwrap_or(rest).to_string());
        }
    }
    if !hit {
        flush_event(&mut cur, &mut out, expect_id);
    }
    out
}

// 事件块并入 out（原始字符串）；若是 expect_id 匹配的最终响应则返回 true
fn flush_event(cur: &mut Vec<String>, out: &mut Vec<String>, expect_id: &Option<Value>) -> bool {
    if cur.is_empty() {
        return false;
    }
    let raw = cur.join("\n");
    cur.clear();
    let matched = serde_json::from_str::<Value>(&raw).map(|v| {
        let is_resp = v.get("id").is_some() && (v.get("result").is_some() || v.get("error").is_some());
        match expect_id {
            Some(id) => is_resp && v.get("id") == Some(&id),
            None => false,
        }
    }).unwrap_or(false);
    out.push(raw);
    matched
}

fn rpc_error_str(req: &Value, code: i64, message: &str, data: Value) -> String {
    json!({
        "jsonrpc": "2.0",
        "id": req.get("id").cloned().unwrap_or(Value::Null),
        "error": {"code": code, "message": message, "data": data}
    })
    .to_string()
}

// 编辑器下线时的标准错误：携带明确警告字段，AI 智能体可直接判定并提示用户启动编辑器
fn editor_offline_error(req: &Value, url: &str, detail: &str, heartbeat_secs: u64) -> String {
    json!({
        "jsonrpc": "2.0",
        "id": req.get("id").cloned().unwrap_or(Value::Null),
        "error": {
            "code": -32002,
            "message": "UE 编辑器 MCP 服务已下线：未检测到运行中的 Unreal 编辑器。需要启动 Unreal 编辑器（并启用 ModelContextProtocol 插件）才能进行 MCP 操作。",
            "data": {
                "status": "editor_offline",
                "editor_required": true,
                "endpoint": url,
                "detail": detail,
                "auto_reconnect": true,
                "heartbeat_secs": heartbeat_secs
            }
        }
    })
    .to_string()
}

// ---------- 心跳线程 ----------

fn heartbeat_loop(up: Arc<Upstream>) {
    let interval = Duration::from_secs(up.heartbeat_secs.max(1));
    loop {
        std::thread::sleep(interval);
        up.probe(); // 在线时零流量；离线时尝试重连并做模拟请求记录
    }
}

// ---------- 主循环 ----------

fn main() {
    let args = parse_args();
    let up = Arc::new(Upstream::new(&args.host, args.port, args.timeout_secs, args.log_dir, args.log_full, args.heartbeat_secs));
    eprintln!(
        "[forwarder] 常驻启动: stdio <-> {}  (心跳 {}s, 转发记录: logs/forward_*.ndjson{})",
        up.url,
        args.heartbeat_secs,
        if args.log_full { ", 全量" } else { ", 大报文截断(--log-full 全量)" }
    );
    // 心跳：随主进程存亡，无独立子进程，不会产生孤儿
    {
        let hb = Arc::clone(&up);
        std::thread::spawn(move || heartbeat_loop(hb));
    }

    let pending: Arc<AtomicUsize> = Arc::new(AtomicUsize::new(0));
    let mut joins = Vec::new();
    for line in std::io::stdin().lock().lines() {
        let line = match line {
            Ok(l) => l,
            Err(_) => break, // stdin 关闭 → 退出
        };
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let msg: Value = match serde_json::from_str(line) {
            Ok(v) => v,
            Err(e) => {
                let err = rpc_error_str(&json!({"id": Value::Null}), -32700, &format!("JSON 解析失败: {e}"), json!({"status":"bad_request"}));
                writeln!(std::io::stdout(), "{err}").ok();
                continue;
            }
        };

        // initialize 内联处理：保证会话建立先于后续请求；其余请求并发转发
        let is_init = msg.get("method").and_then(|m| m.as_str()) == Some("initialize");
        let has_id = msg.get("id").is_some();

        if is_init {
            let replies = up.forward(&msg, line);
            if has_id {
                for reply in replies {
                    writeln!(std::io::stdout(), "{reply}").ok();
                }
                std::io::stdout().flush().ok();
            }
        } else {
            let up2 = Arc::clone(&up);
            let pend = Arc::clone(&pending);
            let body = line.to_string();
            pend.fetch_add(1, Ordering::SeqCst);
            joins.push(std::thread::spawn(move || {
                let replies = up2.forward(&msg, &body);
                // MCP 规范：notification（无 id）不产生响应
                if has_id {
                    for reply in replies {
                        writeln!(std::io::stdout(), "{reply}").ok();
                        std::io::stdout().flush().ok();
                    }
                }
                pend.fetch_sub(1, Ordering::SeqCst);
            }));
        }
    }
    // 等待在途请求完成再退出，避免截断响应
    while pending.load(Ordering::SeqCst) > 0 {
        std::thread::sleep(Duration::from_millis(20));
    }
    for j in joins {
        let _ = j.join();
    }
    eprintln!("[forwarder] stdin 关闭，退出");
}
