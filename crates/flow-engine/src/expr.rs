//! JS 沙箱求值面：脚本、条件表达式与 `${}` 模板展开。
//!
//! 本模块负责一切「跑用户代码」的地方：把 JSON 注入沙箱、执行脚本或
//! 表达式、把结果读回 JSON，并施加预算（堆、栈、输入/输出体积、嵌套
//! 深度、超时）。
//!
//! 它守护的核心不变量有三条：
//! ① **每次求值全新 Runtime**——节点之间零共享状态，决定论与隔离性
//!    同时成立；② **出口语义对齐 JSON.stringify**——升级沙箱实现时行为
//!    不漂移；③ **一切入口都有界**——深嵌套输入必须报错退出，不能打爆
//!    宿主栈（abort 不可捕获）。
//!
//! 不这样做的代价：任一条失守都会变成「节点悄悄跑出了与预览不同的结果」
//! 或「一个脚本把执行器进程带崩」，而执行器崩溃会连带影响同机的其它 run。
use std::time::{Duration, Instant};

use rquickjs::function::Func;
use rquickjs::{Array, BigInt, Context, Ctx, Function, Object, Runtime, Value as JsValue};
use serde_json::Value;

use crate::error::EngineError;
use crate::log_types::{LogLevel, LogStream};
use crate::nodelog::NodeLogger;

/// JS → Rust 递归转换的最大深度：挡手工构造的深链/环，与 JSON.stringify 的
/// 「环即抛错」同类——报错退出，不爆 Rust 栈。
const MAX_JS_DEPTH: usize = 128;

/// 2^53：f64 能精确表示的最大整数（±9007199254740992）。
const MAX_EXACT_F64_INT: i64 = 9_007_199_254_740_992;

/// serde_json::Value → JS 原生值：输入面直接以 JS 对象/数组/标量注入沙箱，
/// 不再 JSON.stringify 成字符串再让 JS 端 JSON.parse 倒一手。
///
/// `define` 是预置的 Object.defineProperty 包装：与 JSON.parse 一致地创建
/// 「自有可枚举数据属性」。Object.set 走 setter 通道，键恰为 "__proto__" 时
/// 会改原型而不是建属性，与 JSON.parse 的语义分叉。
///
/// 大整数走 BigInt：|v| > 2^53 的整数经 JS Number（f64）会静默舍入
/// （9007199254740993 → …992）并沿节点输出向下游传播，转 BigInt 进出
/// 沙箱后透传/比较/模板展开精确无损。代价是脚本中 BigInt 与 Number
/// 混算会抛 TypeError——响亮失败好过静默错值（DESIGN.md §10）。
fn json_to_js<'js>(
    ctx: &Ctx<'js>,
    value: &Value,
    define: &Function<'js>,
) -> Result<JsValue<'js>, rquickjs::Error> {
    Ok(match value {
        Value::Null => JsValue::new_null(ctx.clone()),
        Value::Bool(b) => JsValue::new_bool(ctx.clone(), *b),
        Value::Number(n) => match (n.as_i64(), n.as_u64()) {
            // 超出 f64 精确整数域的 i64：BigInt 进沙箱，出口精确还原
            (Some(v), _) if !(-MAX_EXACT_F64_INT..=MAX_EXACT_F64_INT).contains(&v) => {
                BigInt::from_i64(ctx.clone(), v)?.into_value()
            }
            (Some(v), _) => JsValue::new_number(ctx.clone(), v as f64),
            // serde_json 对超 i64 的正整数存 u64：必然 > 2^53，一律 BigInt
            (None, Some(v)) => BigInt::from_u64(ctx.clone(), v)?.into_value(),
            (None, None) => JsValue::new_number(ctx.clone(), n.as_f64().unwrap_or(f64::NAN)),
        },
        Value::String(s) => rquickjs::String::from_str(ctx.clone(), s)?.into_value(),
        Value::Array(items) => {
            let array = Array::new(ctx.clone())?;
            for (i, item) in items.iter().enumerate() {
                array.set(i, json_to_js(ctx, item, define)?)?;
            }
            array.into()
        }
        Value::Object(map) => {
            let object = Object::new(ctx.clone())?;
            for (k, v) in map {
                let child = json_to_js(ctx, v, define)?;
                define.call::<_, ()>((object.as_value().clone(), k.as_str(), child))?;
            }
            object.into()
        }
    })
}

/// JS 求值结果读取器：把 JS 原生值转回 serde_json::Value，语义对齐
/// JSON.stringify（改造前的结果出口），包括但不限于：
/// - NaN/Infinity → null；
/// - BigInt → 按 i64/u64 精确还原（大整数边界，见 `json_to_js`）；超出
///   JSON 整数域的 BigInt 报错（stringify 同样抛 TypeError）；
/// - 整数值浮点 → 整数（stringify(2.0) 产出 "2" 而非 "2.0"）；
/// - Date（含嵌套位）按 toJSON 约定出 ISO 字符串；
/// - function/symbol/undefined 被丢弃（对象字段缺键、数组成员为 null）；
///
/// `None` 即「该值不可序列化」的传递信使。
struct JsReader<'js> {
    date_ctor: Object<'js>,
    date_to_iso: Function<'js>,
    bigint_to_string: Function<'js>,
    remaining: std::cell::Cell<usize>,
    max_depth: usize,
}

impl<'js> JsReader<'js> {
    /// 捕获求值所需的沙箱内置对象。
    ///
    /// `Date` 构造函数与 BigInt 的 toString 被提前取出，避免在读回路径上
    /// 反复做属性查找，也避免用户脚本覆盖全局 `Date` 后改变出口语义。
    fn new(ctx: &Ctx<'js>) -> Result<Self, rquickjs::Error> {
        Ok(Self {
            date_ctor: ctx.globals().get::<_, Object>("Date")?,
            date_to_iso: ctx.eval::<Function, _>("(function (d) { return d.toISOString(); })")?,
            bigint_to_string: ctx.eval::<Function, _>("(function (b) { return b.toString(); })")?,
            remaining: std::cell::Cell::new(usize::MAX),
            max_depth: MAX_JS_DEPTH,
        })
    }

    /// BigInt → JSON 整数：经十进制字符串回读后按 i64/u64 精确还原。
    /// rquickjs 的 `to_i64` 对超范围值按 JS 语义静默回绕，不能用它判界；
    /// 超出 JSON 整数域（i64/u64）的 BigInt 响亮报错，绝不静默截断。
    fn bigint_to_json(&self, value: &JsValue<'js>) -> Result<Value, rquickjs::Error> {
        let digits: String = self.bigint_to_string.call((value.clone(),))?;
        if let Ok(v) = digits.parse::<i64>() {
            return Ok(Value::from(v));
        }
        if let Ok(v) = digits.parse::<u64>() {
            return Ok(Value::from(v));
        }
        Err(rquickjs::Error::new_from_js_message(
            "bigint",
            "JSON 整数",
            format!("BigInt 超出 i64/u64 范围，拒绝无损序列化：{digits}"),
        ))
    }

    /// 把一个 JS 值读回 JSON 值。
    ///
    /// 返回 `Ok(None)` 表示「该值不可序列化」（function/symbol/undefined），
    /// 调用方据此丢弃该字段或置为 null——与 `JSON.stringify` 的行为一致。
    /// 深度超限属于错误而非丢弃：那是恶意或失控的数据结构。
    fn to_json(
        &self,
        value: &JsValue<'js>,
        depth: usize,
    ) -> Result<Option<Value>, rquickjs::Error> {
        if depth > self.max_depth {
            return Err(rquickjs::Error::new_from_js(
                "object",
                "数据结构嵌套超过 128 层（或存在环），拒绝序列化",
            ));
        }
        self.charge(8)?;
        Ok(Some(match value.type_of() {
            rquickjs::Type::Null | rquickjs::Type::Uninitialized => Value::Null,
            rquickjs::Type::Bool => Value::Bool(value.get::<bool>()?),
            rquickjs::Type::Int => Value::from(value.get::<i32>()? as i64),
            rquickjs::Type::Float => {
                let f = value.get::<f64>()?;
                if !f.is_finite() {
                    Value::Null
                } else if f.fract() == 0.0 && f.abs() <= 9.007_199_254_740_992e15 {
                    // 2^53 是 f64 能精确表示的最大整数：stringify 对它输出整数形式
                    Value::from(f as i64)
                } else {
                    Value::from(f)
                }
            }
            rquickjs::Type::String => {
                let s = value.get::<String>()?;
                self.charge(s.len())?;
                Value::String(s)
            }
            rquickjs::Type::Array => {
                let array = value.get::<Array>()?;
                if array.len() > self.remaining.get() / 8 {
                    return Err(rquickjs::Error::new_from_js(
                        "array",
                        "output budget exceeded",
                    ));
                }
                let mut items = Vec::with_capacity(array.len());
                for i in 0..array.len() {
                    // 稀疏数组的洞读出来是 undefined：对应 stringify 的 [null]
                    let item = array.get::<JsValue>(i)?;
                    items.push(self.to_json(&item, depth + 1)?.unwrap_or(Value::Null));
                }
                Value::Array(items)
            }
            rquickjs::Type::BigInt => self.bigint_to_json(value)?,
            // stringify 丢弃不可序列化值；顶层已由包装器归 null，这里覆盖字段/数组位。
            // 注意 quickjs-ng 的 type_of 把普通函数报成 Constructor（该分支排在
            // Function 之前），构造器与函数一并丢弃。
            rquickjs::Type::Undefined
            | rquickjs::Type::Function
            | rquickjs::Type::Constructor
            | rquickjs::Type::Symbol => return Ok(None),
            // 普通对象、Date，以及 Promise/Exception/Proxy/Module
            // （stringify 视角下都是「按自有可枚举键走查的普通对象」）
            _ => {
                let object = value.get::<Object>()?;
                if object.is_instance_of(&self.date_ctor) {
                    // Date 的 toJSON 约定：ISO 字符串
                    let iso = self
                        .date_to_iso
                        .call::<_, String>((object.as_value().clone(),))?;
                    return Ok(Some(Value::String(iso)));
                }
                let mut map = serde_json::Map::new();
                for key in object.keys::<String>() {
                    let key = key?;
                    self.charge(key.len())?;
                    let item = object.get::<_, JsValue>(&key)?;
                    if let Some(json) = self.to_json(&item, depth + 1)? {
                        map.insert(key, json);
                    }
                }
                Value::Object(map)
            }
        }))
    }
    /// 从输出预算里扣减 n 字节。
    ///
    /// 预算是「读回」侧的粗略计量：每个值记 8 字节，字符串与键另按长度
    /// 扣。目的是在结果变大到不可接受之前就中止，而不是等序列化完再判体积。
    fn charge(&self, n: usize) -> Result<(), rquickjs::Error> {
        // usize::MAX 是「未设预算」的哨兵值：无上限时不扣减，直接放行。
        if self.remaining.get() == usize::MAX {
            return Ok(());
        }
        let remaining = self
            .remaining
            .get()
            .checked_sub(n)
            .ok_or_else(|| rquickjs::Error::new_from_js("value", "output budget exceeded"))?;
        self.remaining.set(remaining);
        Ok(())
    }
}

/// 表达式求值统一入口：JS 沙箱，无 IO，带超时中断。
///
/// 走 JS 而不是自造 DSL：前端预览和引擎求值共用同一种语言。
/// 每次求值都是全新的 Runtime + Context：全局表、原子表随运行期生命周期
/// 一起回收，节点之间不共享任何可变状态（决定论与隔离性）。
/// 不跨求值复用 Runtime 是刻意的：quickjs 的原子表只增不减，而复用后输入
/// 数据里的任意 JSON 键都会沉淀成原子——长生命周期进程下这是无界增长；
/// 把整个 Runtime 从「盘古开天」重新实例化，换回来的正是这张表的清零。
/// console.log → LogLevel 映射（与 Node 约定一致：error/warn 走 stderr）。
/// 上面这段描述的是整个求值面的设计；下面两个函数是它的最小实现：
/// 级别名 → [`LogLevel`]、级别名 → [`LogStream`]。分成两张表而不是一张，
/// 因为「级别」与「去向」是两个独立维度（info 也可能需要走 stderr）。
fn console_level(name: &str) -> LogLevel {
    match name {
        "debug" => LogLevel::Debug,
        "warn" => LogLevel::Warn,
        "error" => LogLevel::Error,
        _ => LogLevel::Info,
    }
}

/// console 级别名 → 输出流：warn/error 走 stderr，其余走 stdout。
///
/// 与 Node 的常规约定一致：把「需要人工看的」与「流水」分开，
/// 采集端按流分开处理时无需再解析内容。
fn console_stream(name: &str) -> LogStream {
    match name {
        "warn" | "error" => LogStream::Stderr,
        _ => LogStream::Stdout,
    }
}

/// 录制运行（regression）用的确定性上下文：固定时钟与随机种子。
///
/// 回归要能重放同一条路径，就不能让 `Date.now()` 与 `Math.random()`
/// 随真实时间变化——否则每次回归的节点输出都不同，基线无法比对。
#[derive(Clone, Copy)]
pub struct JsDeterminism {
    /// 冻结的 Unix 毫秒时间戳，`Date.now()` 与无参 `new Date()` 都用它。
    pub clock_ms: i64,
    /// 随机种子，决定 `Math.random()` 的序列。
    pub seed: u32,
}

/// 沙箱的资源预算集合。
///
/// 这是**单一定义处**：本地执行与远程执行共用同一组数字——否则会出现
/// 「本机能跑、远程 OOM」这类无法复现的问题（DESIGN.md §10.1）。
#[derive(Clone, Copy)]
pub struct JsLimits {
    /// 输入面总字节预算（input/nodes/tpl 三者共享）。
    pub input_bytes: usize,
    /// QuickJS 堆上限。
    pub heap_bytes: usize,
    /// 输出面字节预算。
    pub output_bytes: usize,
    /// JSON 嵌套深度上限。
    pub depth: usize,
    /// 确定性上下文；`None` 表示按真实时间/随机执行。
    pub determinism: Option<JsDeterminism>,
}

/// 默认预算：8 MiB 输入 / 32 MiB 堆 / 8 MiB 输出 / 深度 64。
///
/// 每一项都有界是刻意的——深嵌套与大对象在无界入口下会直接打爆原生栈
/// 或宿主内存，而那类崩溃是不可捕获的 abort，只能靠预防。
impl Default for JsLimits {
    /// 返回默认预算（8 MiB 输入 / 32 MiB 堆 / 8 MiB 输出 / 深度 64）。
    fn default() -> Self {
        Self {
            input_bytes: 8 * 1024 * 1024,
            heap_bytes: 32 * 1024 * 1024,
            output_bytes: 8 * 1024 * 1024,
            depth: 64,
            determinism: None,
        }
    }
}

/// 统一的受限求值实现：所有求值入口（脚本、条件、模板）都走这里。
///
/// 流程固定为「预算校验 → 建 Runtime → 设内存/栈/超时 → 注入输入 →
/// 执行 → 读回 → 复核输出体积」。每个求值都是全新的 Runtime，绝不复用。
///
/// # Errors
/// 输入超预算、沙箱创建失败、脚本异常、超时、输出超预算或序列化失败时
/// 返回 `EngineError::Expr`，消息面向用户可直接展示。
fn run_js_limited(
    body: &str,
    input: &Value,
    nodes: &Value,
    tpl: &Value,
    timeout: Duration,
    logger: &NodeLogger,
    limits: Option<JsLimits>,
) -> Result<Value, EngineError> {
    if let Some(limits) = limits {
        // 输入面预算由 input/nodes/tpl 三者**共享**：三者都来自上游
        // 数据，任意一个塞满预算都足以拖垮下游，所以是总账而非分账。
        let mut remaining = limits.input_bytes;
        for value in [input, nodes, tpl] {
            // 先查深度再算体积：深链的体积可能不大，但序列化/转换仍会
            // 递归爆栈，体积检查拦不住它。
            flow_journal::codec::validate_depth(value, 0)
                .map_err(|e| EngineError::Expr(e.to_string()))?;
            let bytes = flow_journal::codec::bounded_json(value, remaining)
                .map_err(|e| EngineError::Expr(e.to_string()))?;
            remaining = remaining.saturating_sub(bytes.len());
        }
    }
    // 每次求值新建 Runtime：原子表只增不复用，清零由「重新实例化」换来。
    let runtime = Runtime::new().map_err(|e| EngineError::Expr(e.to_string()))?;
    if let Some(limits) = limits {
        runtime.set_memory_limit(limits.heap_bytes);
        // 栈上限 512 KiB：深层递归在 JS 侧表现为宿主栈溢出（abort，
        // 不可捕获），只能在进沙箱前就限制住。
        runtime.set_max_stack_size(512 * 1024);
    }
    // 超时用中断处理器实现：quickjs 会在字节码边界回调它，
    // 长循环因此能被打断而不是把执行槽永久占住。
    let deadline = Instant::now() + timeout;
    runtime.set_interrupt_handler(Some(Box::new(move || Instant::now() > deadline)));

    let context = Context::full(&runtime).map_err(|e| EngineError::Expr(e.to_string()))?;

    // 用户代码包在两层函数里：内层执行 body，外层 try/catch 把 JS 异常
    // 转成 {ok:false} 结构——异常是**业务失败**，不能让它以沙箱级错误
    // 的面目出现（那会被误判成平台故障）。
    let program = format!(
        r#"(function () {{
  try {{
    var __v = (function () {{
{body}
    }})();
    return {{ ok: true, value: (__v === undefined ? null : __v) }};
  }} catch (e) {{
    return {{ ok: false, error: String((e && e.message) || e) }};
  }}
}})()"#
    );

    let outcome = context.with(|ctx| -> Result<Result<Value, String>, rquickjs::Error> {
        let globals = ctx.globals();
        // 录制（回归）模式：替换全局 Date 与 Math.random，使同一份代码
        // 在任何时刻执行都得到同一结果。时间统一按 UTC 解释，避免机器
        // 本地时区把基线与候选拉开。
        if let Some(recorded) = limits.and_then(|l| l.determinism) {
            let install = format!(r#"(function () {{
                const NativeDate = Date, clock = {}, initialSeed = {};
                function parseRecorded(text) {{
                    if (typeof text !== 'string' || !/^\d{{4}}-\d{{2}}-\d{{2}}(?:T.*(?:Z|[+-]\d{{2}}:\d{{2}}))?$/.test(text)) throw new Error('recorded Date parsing requires ISO date or explicit timezone');
                    return NativeDate.parse(text);
                }}
                function RecordedDate(...args) {{
                    if (!new.target) return new NativeDate(clock).toISOString();
                    let values = args.length > 1 ? [NativeDate.UTC(...args)] : args.length ? [typeof args[0] === 'string' ? parseRecorded(args[0]) : args[0]] : [clock];
                    return Reflect.construct(NativeDate, values, new.target);
                }}
                RecordedDate.prototype = NativeDate.prototype;
                Object.defineProperty(RecordedDate.prototype, 'constructor', {{value: RecordedDate}});
                Object.defineProperties(RecordedDate, {{
                    now: {{value: () => clock}}, parse: {{value: parseRecorded}}, UTC: {{value: NativeDate.UTC}}
                }});
                for (const name of ['FullYear','Month','Date','Day','Hours','Minutes','Seconds','Milliseconds']) {{
                    const p = NativeDate.prototype;
                    p['get' + name] = p['getUTC' + name];
                    if (p['setUTC' + name]) p['set' + name] = p['setUTC' + name];
                }}
                NativeDate.prototype.getTimezoneOffset = () => 0;
                NativeDate.prototype.getYear = function () {{return this.getUTCFullYear() - 1900}};
                NativeDate.prototype.setYear = function (year) {{return this.setUTCFullYear(year >= 0 && year <= 99 ? Number(year) + 1900 : year)}};
                NativeDate.prototype.toString = NativeDate.prototype.toISOString;
                NativeDate.prototype.toDateString = function () {{return this.toISOString().slice(0,10)}};
                NativeDate.prototype.toTimeString = function () {{return this.toISOString().slice(11)}};
                NativeDate.prototype.toLocaleString = NativeDate.prototype.toISOString;
                NativeDate.prototype.toLocaleDateString = function () {{return this.toISOString().slice(0,10)}};
                NativeDate.prototype.toLocaleTimeString = function () {{return this.toISOString().slice(11,23)}};
                globalThis.Date = RecordedDate;
                let state = initialSeed || 1;
                Object.defineProperty(Math, 'random', {{configurable:false, writable:false, value: () => {{
                    state ^= state << 13; state ^= state >>> 17; state ^= state << 5;
                    return (state >>> 0) / 4294967296;
                }}}});
            }})();"#, recorded.clock_ms, recorded.seed);
            ctx.eval::<(), _>(install)?;
        }
        // 与 JSON.parse 一致的属性定义（__proto__ 等键不会触发 setter）
        let define = ctx.eval::<Function, _>(
            "(function (o, k, v) { Object.defineProperty(o, k, { value: v, enumerable: true, writable: true, configurable: true }); })",
        )?;
        // 沙箱里只有这三个全局是数据入口：脚本、模板与节点输出。刻意
        // 不暴露别的宿主能力——没有文件、没有网络、没有 require。
        globals.set("input", json_to_js(&ctx, input, &define)?)?;
        globals.set("nodes", json_to_js(&ctx, nodes, &define)?)?;
        globals.set("tpl", json_to_js(&ctx, tpl, &define)?)?;

        // console 桥接：宿主函数收（级别，已格式化消息），格式化在 JS 侧完成
        //（多参数拼串、对象 JSON.stringify、循环引用回退 String()）。
        // 所有人都有 console：条件/模板求值传 disabled logger，引用 console
        // 不再 ReferenceError，只是没人听。
        let emit_logger = logger.clone();
        globals.set(
            "__flow_console_emit",
            Func::from(move |level: String, message: String| {
                emit_logger.log(console_level(&level), console_stream(&level), message);
            }),
        )?;
        let console: JsValue = ctx.eval(
            r#"(function (emit) {
  function fmt() {
    var out = [];
    for (var i = 0; i < arguments.length; i++) {
      var v = arguments[i];
      if (typeof v === 'string') { out.push(v); continue; }
      try { out.push(JSON.stringify(v)); } catch (e) { out.push(String(v)); }
    }
    return out.join(' ');
  }
  return {
    log: function () { emit('info', fmt.apply(null, arguments)); },
    info: function () { emit('info', fmt.apply(null, arguments)); },
    debug: function () { emit('debug', fmt.apply(null, arguments)); },
    warn: function () { emit('warn', fmt.apply(null, arguments)); },
    error: function () { emit('error', fmt.apply(null, arguments)); }
  };
})(__flow_console_emit)"#,
        )?;
        globals.set("console", console)?;

        let result: JsValue = ctx.eval(program)?;
        let outcome = result.get::<Object>()?;
        let mut reader = JsReader::new(&ctx)?;
        // 读回前才装上输出预算与深度上限：读回路径没有预算就等于
        // 「结果可以无限大」，那会在序列化阶段把执行器内存吃光。
        if let Some(limits)=limits {reader.remaining.set(limits.output_bytes);reader.max_depth=limits.depth;}
        if outcome.get::<_, bool>("ok")? {
            let value = outcome.get::<_, JsValue>("value")?;
            Ok(Ok(reader.to_json(&value, 0)?.unwrap_or(Value::Null)))
        } else {
            Ok(Err(outcome.get::<_, String>("error")?))
        }
    });
    match outcome.map_err(|e| EngineError::Expr(e.to_string()))? {
        Ok(value) => {
            if let Some(limits) = limits {
                // 读回预算只是粗略计量，最终体积以实际序列化字节为准：
                // 读回侧记账宽松时，这里是最后一道闸门。
                flow_journal::codec::bounded_json(&value, limits.output_bytes)
                    .map_err(|e| EngineError::Expr(e.to_string()))?;
            }
            Ok(value)
        }
        Err(message) => Err(EngineError::Expr(message)),
    }
}

/// 脚本节点：body 是一段带 `return` 的函数体，可用 `input` 与 `nodes`。
/// `logger` 承接脚本内的 console.* 输出（level 透传，log/info→stdout，warn/error→stderr）。
///
/// 默认预算（32 MiB 堆 / 512 KiB 栈 / 深度 64 / 入出
/// 8 MiB）：不留无界入口——深嵌套输入会直接打爆原生栈（abort，不可捕获），
/// 「无限制」在这个沙箱面里只能是事故，不是特性。
pub fn eval_body(
    body: &str,
    input: &Value,
    nodes: &Value,
    timeout: Duration,
    logger: &NodeLogger,
) -> Result<Value, EngineError> {
    eval_body_recorded(body, input, nodes, timeout, logger, None)
}

/// 脚本节点的录制变体：注入确定性时钟与随机种子后执行同一段 body。
///
/// 与 [`eval_body`] 的差别只有上下文（时间/随机），预算规则完全相同——
/// 回归执行与正常执行走同一条路径，才能保证基线可信。
///
/// # Errors
/// 同 [`eval_body`]。
pub fn eval_body_recorded(
    body: &str,
    input: &Value,
    nodes: &Value,
    timeout: Duration,
    logger: &NodeLogger,
    determinism: Option<JsDeterminism>,
) -> Result<Value, EngineError> {
    run_js_limited(
        body,
        input,
        nodes,
        &Value::Null,
        timeout,
        logger,
        Some(JsLimits {
            determinism,
            ..JsLimits::default()
        }),
    )
}

/// 条件节点：求值单个表达式。console 可用（disabled logger，输出丢弃）。
/// 预算规则与 [`eval_body`] 相同：默认有界，不存在无界求值入口。
pub fn eval_expr(
    expr: &str,
    input: &Value,
    nodes: &Value,
    timeout: Duration,
) -> Result<Value, EngineError> {
    let body = format!("    return ({expr});");
    run_js_limited(
        &body,
        input,
        nodes,
        &Value::Null,
        timeout,
        &NodeLogger::disabled(),
        Some(JsLimits::default()),
    )
}

/// 展开字符串中的 `${expr}` 模板。所有节点 params 的统一前置展开
/// （`exec::expand_params`；script.code / condition.expr 等 opaque 字段除外）。
///
/// 两条规则，按「整个值是否恰为一个模板」分流：
///
/// 1. **整值模板**（`"${expr}"` 独占整个字符串）：按求值结果的**原类型**
///    替换——数字还是数字、对象还是对象。`input_mapping` 之类的结构化
///    传参依赖这条；`undefined` 归一为 `null`（与 `eval_body` 出口一致）。
/// 2. **插值模板**（模板嵌在更长文本里）：字符串语义，非字符串结果经
///    `JSON.stringify` 拼回原位（url/headers 的历史行为，逐字不变）。
///
/// 规则 1 只影响「整值 + 非字符串」这一格：整值字符串模板（如
/// `"${input.token}"`）在两条规则下结果相同，存量定义零行为变化。
pub fn expand_templates(
    tpl: &Value,
    input: &Value,
    nodes: &Value,
    timeout: Duration,
) -> Result<Value, EngineError> {
    expand_templates_impl(tpl, input, nodes, timeout, Some(JsLimits::default()))
}

/// 模板展开的录制变体：注入确定性上下文后再展开。
///
/// 与 [`expand_templates`] 的差别只有时钟/随机，展开规则完全一致——
/// 回归时模板必须求值出与基线相同的结果，否则整条基线都会失配。
///
/// # Errors
/// 同 [`expand_templates`]。
pub fn expand_templates_recorded(
    tpl: &Value,
    input: &Value,
    nodes: &Value,
    timeout: Duration,
    determinism: Option<JsDeterminism>,
) -> Result<Value, EngineError> {
    expand_templates_impl(
        tpl,
        input,
        nodes,
        timeout,
        Some(JsLimits {
            determinism,
            ..JsLimits::default()
        }),
    )
}
/// 模板展开的内部实现：展开逻辑写成一段 JS 在沙箱里跑。
///
/// 展开放在沙箱内而不是宿主侧逐值求值，是为了让 `${}` 里的表达式与
/// 脚本节点、条件节点**用同一种语言**：用户在预览里写通的表达式，
/// 到执行期必须得到同一个答案。
///
/// # Errors
/// 模板表达式求值失败、展开结果超预算时返回 `EngineError::Expr`。
fn expand_templates_impl(
    tpl: &Value,
    input: &Value,
    nodes: &Value,
    timeout: Duration,
    limits: Option<JsLimits>,
) -> Result<Value, EngineError> {
    let body = r#"    function __one(e) {
      var r = eval(e);
      return r === undefined ? null : r;
    }
    function __interp(v, input, nodes) {
      return v.replace(/\$\{([^}]+)\}/g, function (_, e) {
        var r = __one(e);
        // BigInt 走 toString 拿精确十进制（雪花 ID 拼 URL）：JSON.stringify
        // 对 BigInt 抛错，而 f64 老路径在这里是静默舍入。
        if (typeof r === 'bigint') { return r.toString(); }
        return (typeof r === 'string') ? r : JSON.stringify(r);
      });
    }
    function __expand(v, input, nodes) {
      if (typeof v === 'string') {
        var whole = /^\$\{([^}]+)\}$/.exec(v);
        return whole ? __one(whole[1]) : __interp(v, input, nodes);
      }
      if (Array.isArray(v)) { return v.map(function (x) { return __expand(x, input, nodes); }); }
      if (v && typeof v === 'object') {
        var o = Object.create(null);
        Object.keys(v).forEach(function (k) { o[k] = __expand(v[k], input, nodes); });
        return o;
      }
      return v;
    }
    return __expand(tpl, input, nodes);"#;
    run_js_limited(
        body,
        input,
        nodes,
        tpl,
        timeout,
        &NodeLogger::disabled(),
        limits,
    )
}

#[cfg(test)]
mod tests;
