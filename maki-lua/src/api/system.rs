//! `maki.system`, Neovim's `vim.system`: a child process whose stdio reaches
//! the plugin in raw chunks, enough to speak a protocol like LSP over it.

use std::cell::RefCell;
use std::collections::HashMap;
use std::process::{Command, ExitStatus};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, Weak};
use std::time::Duration;

use flume::{Receiver, Sender};
use futures_lite::future::{or, pending, zip};
use futures_lite::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use maki_agent::ChildGuard;
use maki_lua_macro::{lua_class, lua_fn};
use mlua::{
    BString, Function, Lua, LuaString, Result as LuaResult, Table, UserDataFields, UserDataRef,
    Value,
};
use smol::Timer;
use smol::process::{ChildStdin, Command as AsyncCommand, Stdio};

use crate::api::util::pair::{Pair, err_pair, pair, try_pair};
use crate::api::util::process::{SIGKILL, SIGTERM, isolate, signal_group};
use crate::runtime::{
    active_task_id, enqueue_spawned_task, plugin_spawns_cancelled, strip_traceback,
};

/// Anything past this waits in the pipe, so a plugin that stops reading
/// stalls the child instead of growing a buffer.
const READ_CHUNK: usize = 64 * 1024;
/// Chunks read ahead of the callback that is running.
const QUEUED_CHUNKS: usize = 1;
const STDOUT: usize = 0;
const STDERR: usize = 1;
const TIMEOUT_EXIT_CODE: i32 = 124;
const UNKNOWN_EXIT_CODE: i32 = -1;
/// A process whose driver was cancelled with its plugin, which kills it.
const KILLED: Completed = Completed {
    code: 0,
    signal: SIGKILL,
    stdout: None,
    stderr: None,
};
const EMPTY_CMD_ERR: &str = "maki.system: cmd must be a non-empty list of strings";
const STDIN_TYPE_ERR: &str = "maki.system: stdin must be a boolean or a string";
const OUTPUT_TYPE_ERR: &str = "maki.system: stdout and stderr must be a function or a boolean";
const WAIT_IN_CALLBACK_ERR: &str = "wait: called from an output callback of the same process, which cannot finish until it returns";
const CANNOT_START_ERR: &str = "cannot start";
const WRITE_FAILED_ERR: &str = "write failed";
const PLUGIN_UNLOADING_ERR: &str = "plugin is unloading";
const STDIN_CLOSED_ERR: &str = "stdin is not open";
const PROCESS_EXITED_ERR: &str = "process has exited";
const NOT_REAPED_ERR: &str = "process did not exit after SIGKILL";

thread_local! {
    static PLUGIN_SYSTEMS: RefCell<HashMap<Arc<str>, Vec<Weak<SystemState>>>> = RefCell::default();
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|e| e.into_inner())
}

#[derive(Clone)]
struct Completed {
    code: i32,
    signal: i32,
    stdout: Option<BString>,
    stderr: Option<BString>,
}

impl Completed {
    fn to_table(&self, lua: &Lua) -> LuaResult<Table> {
        let table = lua.create_table()?;
        table.set("code", self.code)?;
        table.set("signal", self.signal)?;
        table.set("stdout", self.stdout.clone())?;
        table.set("stderr", self.stderr.clone())?;
        Ok(table)
    }
}

#[cfg(unix)]
fn exit_signal(status: &ExitStatus) -> i32 {
    use std::os::unix::process::ExitStatusExt;
    status.signal().unwrap_or_default()
}

#[cfg(not(unix))]
fn exit_signal(_status: &ExitStatus) -> i32 {
    Default::default()
}

struct StdinWrite {
    data: Vec<u8>,
    /// `None` for a write made from the process's own callback: nobody waits.
    flushed: Option<Sender<Result<(), String>>>,
}

struct SystemState {
    pid: u32,
    /// `None` when not piped or closed, which lets the writer drain and close.
    stdin: Mutex<Option<Sender<StdinWrite>>>,
    /// A callback is still listening, so the process outlives its handle.
    watched: bool,
    timed_out: AtomicBool,
    /// The task running the callbacks, where `write` must not wait.
    driver_task: OnceLock<u64>,
    /// Set once the child is reaped, after which its pid is never signalled.
    completed: Mutex<Option<Completed>>,
    /// Disconnects once `on_exit` has returned.
    done: Receiver<()>,
}

impl SystemState {
    fn completed(&self) -> MutexGuard<'_, Option<Completed>> {
        lock(&self.completed)
    }

    fn in_driver_task(&self, lua: &Lua) -> bool {
        self.driver_task
            .get()
            .is_some_and(|task| active_task_id(lua) == Some(*task))
    }

    async fn finished_within(&self, limit: Option<Duration>) -> bool {
        let timeout = async {
            limit.map_or_else(Timer::never, Timer::after).await;
            false
        };
        or(async { self.done.recv_async().await.is_err() }, timeout).await
    }

    /// The group leader stays a zombie until the driver reaps it, so its pid
    /// is the right target up to then.
    fn signal(&self, signal: i32) {
        let completed = self.completed();
        if completed.is_none() {
            signal_group(self.pid, signal);
        }
    }
}

/// Owned by the driver. Answers every `wait` once dropped, also when the
/// driver was cancelled before it reaped the child.
struct Settle {
    state: Arc<SystemState>,
    _done: Sender<()>,
}

impl Drop for Settle {
    fn drop(&mut self) {
        self.state.completed().get_or_insert(KILLED);
        lock(&self.state.stdin).take();
    }
}

pub(crate) struct System(Arc<SystemState>);

impl Drop for System {
    fn drop(&mut self) {
        if !self.0.watched {
            self.0.signal(SIGKILL);
        }
    }
}

pub(crate) fn kill_plugin_systems(plugin: &str) {
    let systems = PLUGIN_SYSTEMS.with_borrow_mut(|systems| systems.remove(plugin));
    for state in systems.iter().flatten().filter_map(Weak::upgrade) {
        state.signal(SIGKILL);
    }
}

fn track(plugin: Arc<str>, state: &Arc<SystemState>) {
    PLUGIN_SYSTEMS.with_borrow_mut(|systems| {
        let running = systems.entry(plugin).or_default();
        running.retain(|state| state.strong_count() > 0);
        running.push(Arc::downgrade(state));
    });
}

enum Sink {
    Collect(Vec<u8>),
    Callback(Function),
}

/// `None` discards the stream.
fn parse_output(value: Value) -> LuaResult<Option<Sink>> {
    match value {
        Value::Nil | Value::Boolean(true) => Ok(Some(Sink::Collect(Vec::new()))),
        Value::Boolean(false) => Ok(None),
        Value::Function(callback) => Ok(Some(Sink::Callback(callback))),
        _ => Err(mlua::Error::runtime(OUTPUT_TYPE_ERR)),
    }
}

/// Sends each chunk read from {pipe}, then `None` at the end of the stream.
async fn pump(
    pipe: Option<impl AsyncRead + Unpin>,
    stream: usize,
    tx: Sender<(usize, Option<Vec<u8>>)>,
) {
    let Some(mut pipe) = pipe else {
        return;
    };
    let mut buf = vec![0; READ_CHUNK];
    loop {
        let chunk = match pipe.read(&mut buf).await {
            Ok(0) => None,
            Ok(n) => Some(buf[..n].to_vec()),
            Err(e) => {
                tracing::warn!(error = %e, "maki.system: output cut short");
                None
            }
        };
        let eof = chunk.is_none();
        if tx.send_async((stream, chunk)).await.is_err() || eof {
            return;
        }
    }
}

/// Writes apart from the callbacks, so a write never waits on Lua to drain
/// the child's output.
async fn write_stdin(stdin: Option<(ChildStdin, Receiver<StdinWrite>)>) {
    let Some((mut pipe, writes)) = stdin else {
        return;
    };
    while let Ok(StdinWrite { data, flushed }) = writes.recv_async().await {
        let result = async {
            pipe.write_all(&data).await?;
            pipe.flush().await
        }
        .await
        .map_err(|e| format!("{WRITE_FAILED_ERR}: {e}"));
        match (flushed, result) {
            (Some(flushed), result) => drop(flushed.send(result)),
            (None, Err(error)) => tracing::debug!(error, "maki.system: queued stdin write failed"),
            (None, Ok(())) => {}
        }
    }
    let _ = pipe.close().await;
}

/// A raising callback must not stop the stream: the next message from a
/// server is as good as the one that tripped the handler.
fn log_callback_error(plugin: &str, result: LuaResult<()>) {
    if let Err(e) = result {
        tracing::warn!(plugin, error = %strip_traceback(&e), "maki.system callback failed");
    }
}

/// Spawns the child and returns it with its driver: a function that owns
/// the child while it runs, writes its stdin, hands its output to the
/// plugin, reaps it and calls `on_exit`. It runs as a `maki.async.spawn`
/// task, so a plugin unload cancels it and the `ChildGuard` kills the child.
fn start(
    lua: &Lua,
    plugin: Arc<str>,
    cmd: Vec<String>,
    opts: Option<Table>,
    on_exit: Option<Function>,
) -> LuaResult<Pair<(System, Function)>> {
    let (program, args) = cmd
        .split_first()
        .ok_or_else(|| mlua::Error::runtime(EMPTY_CMD_ERR))?;
    let opts = opts.map_or_else(|| lua.create_table(), Ok)?;
    let (tx, writes) = flume::unbounded();
    let (piped_stdin, stdin) = match opts.get("stdin")? {
        Value::Nil | Value::Boolean(false) => (false, None),
        Value::Boolean(true) => (true, Some(tx)),
        Value::String(data) => {
            let data = data.as_bytes().to_vec();
            let _ = tx.send(StdinWrite {
                data,
                flushed: None,
            });
            (true, None)
        }
        _ => return Err(mlua::Error::runtime(STDIN_TYPE_ERR)),
    };
    let mut sinks = [
        parse_output(opts.get("stdout")?)?,
        parse_output(opts.get("stderr")?)?,
    ];
    let env: Option<HashMap<String, String>> = opts.get("env")?;
    let mut command = Command::new(program);
    try_pair!(isolate(
        &mut command,
        opts.get::<Option<String>>("cwd")?.as_deref()
    ));
    command.args(args).envs(env.into_iter().flatten());
    let piped = |piped: bool| if piped { Stdio::piped() } else { Stdio::null() };
    let mut child = try_pair!(
        AsyncCommand::from(command)
            .stdin(piped(piped_stdin))
            .stdout(piped(sinks[STDOUT].is_some()))
            .stderr(piped(sinks[STDERR].is_some()))
            .spawn()
            .map_err(|e| format!("{CANNOT_START_ERR} {program}: {e}"))
    );

    let (done_tx, done) = flume::bounded(0);
    let state = Arc::new(SystemState {
        pid: child.id(),
        stdin: Mutex::new(stdin),
        watched: on_exit.is_some()
            || sinks
                .iter()
                .flatten()
                .any(|s| matches!(s, Sink::Callback(_))),
        timed_out: AtomicBool::new(false),
        driver_task: OnceLock::new(),
        completed: Mutex::new(None),
        done,
    });
    track(Arc::clone(&plugin), &state);
    let stdin = child.stdin.take().map(|pipe| (pipe, writes));
    let (stdout, stderr) = (child.stdout.take(), child.stderr.take());
    let mut child = ChildGuard::new(child);
    let settle = Settle {
        state: Arc::clone(&state),
        _done: done_tx,
    };
    let drive = move |lua: Lua| async move {
        let state = &settle.state;
        if let Some(task) = active_task_id(&lua) {
            let _ = state.driver_task.set(task);
        }
        let (tx, rx) = flume::bounded(QUEUED_CHUNKS);
        let pumps = zip(pump(stdout, STDOUT, tx.clone()), pump(stderr, STDERR, tx));
        let deliver = async {
            while let Ok((stream, chunk)) = rx.recv_async().await {
                match &mut sinks[stream] {
                    Some(Sink::Collect(bytes)) => bytes.extend(chunk.into_iter().flatten()),
                    Some(Sink::Callback(callback)) => {
                        let data = chunk.map(BString::from);
                        let result = callback.call_async::<()>((Value::Nil, data)).await;
                        log_callback_error(&plugin, result);
                    }
                    None => {}
                }
            }
        };
        let reap = async {
            zip(pumps, deliver).await;
            child.status().await
        };
        let writer = async {
            write_stdin(stdin).await;
            pending().await
        };
        let (code, signal) = or(reap, writer).await.map_or((UNKNOWN_EXIT_CODE, 0), |s| {
            (s.code().unwrap_or_default(), exit_signal(&s))
        });
        lock(&state.stdin).take();
        let mut collected = |stream: usize| match sinks[stream].take() {
            Some(Sink::Collect(bytes)) => Some(BString::from(bytes)),
            _ => None,
        };
        let completed = Completed {
            code: if state.timed_out.load(Ordering::Acquire) {
                TIMEOUT_EXIT_CODE
            } else {
                code
            },
            signal,
            stdout: collected(STDOUT),
            stderr: collected(STDERR),
        };
        *state.completed() = Some(completed.clone());
        if let Some(on_exit) = on_exit {
            let result = on_exit.call_async::<()>(completed.to_table(&lua)?).await;
            log_callback_error(&plugin, result);
        }
        Ok(())
    };
    let drive = Mutex::new(Some(drive));
    let driver = lua.create_async_function(move |lua, ()| {
        let drive = lock(&drive).take();
        async move {
            match drive {
                Some(drive) => drive(lua).await,
                None => Ok(()),
            }
        }
    })?;
    Ok((Some((System(state), driver)), None))
}

/// Run {cmd} as a child process, like Neovim's `vim.system`, so a plugin
/// can talk to a server over stdio and lift most of `vim/lsp/rpc.lua`.
///
/// Callbacks get raw chunks in order, one at a time, and may yield. Output
/// is read at most one chunk ahead of them, so a slow callback slows the
/// child down instead of filling memory. The process outlives the call that
/// started it. Unloading the plugin kills it with its process group, and so
/// does garbage collecting the handle of a process with no callback.
///
/// {opts} fields:
///   `cwd` (string) Working directory (tilde is expanded).
///   `env` (table) Variables added to the environment, `{ VAR = "value" }`.
///   `stdin` (boolean|string) `true` opens a pipe for `obj:write()`. A
///     string is written and then stdin is closed. Default: no stdin.
///   `stdout` (function|boolean) `fun(err, data)` gets each chunk, then
///     `data = nil` at the end of the stream. `true` (default) collects the
///     output into the completed table, `false` discards it.
///   `stderr` (function|boolean) Same as `stdout`.
///
/// {on_exit} gets the completed table `{ code, signal, stdout?, stderr? }`
/// once the process has exited and its output has ended. `stdout` and
/// `stderr` are only set for collected streams.
///
/// Differences from Neovim: failures return nil plus an error, `write` and
/// `wait` yield instead of blocking, and `text`, `timeout`, `clear_env`,
/// `detach` and signal names are not supported.
///
/// @param cmd string[] Program and arguments, like `{ "rust-analyzer" }`. No shell is involved.
/// @param opts table? Options (see above).
/// @param on_exit function? Called with the completed table.
/// @return (maki.SystemObj?, string?) The process, or nil plus an error string.
/// @example
/// local server, err = maki.system({ "rust-analyzer" }, {
///   stdin = true,
///   stdout = function(_, data)
///     if data then decoder:feed(data) end
///   end,
/// }, function(done) print("server exited", done.code) end)
/// if not server then return maki.log.error(err) end
/// server:write(frame)
#[lua_fn(guard = Run)]
fn system(
    lua: &Lua,
    #[ctx] plugin: Arc<str>,
    cmd: Vec<String>,
    opts: Option<Table>,
    on_exit: Option<Function>,
) -> LuaResult<Pair<System>> {
    if plugin_spawns_cancelled(lua, &plugin) {
        return Ok(err_pair(PLUGIN_UNLOADING_ERR));
    }
    let (system, driver) = match start(lua, Arc::clone(&plugin), cmd, opts, on_exit)? {
        (Some(started), _) => started,
        (None, err) => return Ok((None, err)),
    };
    lua.create_registry_value(driver)
        .and_then(|driver| enqueue_spawned_task(lua, plugin, driver))
        .inspect_err(|_| system.0.signal(SIGKILL))?;
    Ok((Some(system), None))
}

/// Write {data} to stdin, or close it with `nil` once earlier writes are
/// out. Yields until written, except in the process's own callbacks, where
/// it queues the data and returns at once so the child cannot deadlock on
/// output the callback has yet to read.
///
/// @param data string? Bytes to send, or nil to close stdin.
/// @return (boolean?, string?) `true`, or nil plus an error string.
/// @example
/// local ok, err = obj:write(header .. body)
/// if not ok then return maki.log.error(err) end
#[lua_fn]
async fn write(
    lua: Lua,
    this: UserDataRef<System>,
    data: Option<LuaString>,
) -> LuaResult<Pair<bool>> {
    let state = Arc::clone(&this.0);
    drop(this);
    let answer = {
        let mut stdin = lock(&state.stdin);
        let Some(writes) = stdin.as_ref() else {
            return Ok(err_pair(STDIN_CLOSED_ERR));
        };
        let Some(data) = data else {
            *stdin = None;
            return Ok((Some(true), None));
        };
        let (flushed, answer) = (!state.in_driver_task(&lua))
            .then(|| flume::bounded(1))
            .unzip();
        let data = data.as_bytes().to_vec();
        if writes.send(StdinWrite { data, flushed }).is_err() {
            return Ok(err_pair(PROCESS_EXITED_ERR));
        }
        answer
    };
    let Some(answer) = answer else {
        return Ok((Some(true), None));
    };
    let written = answer.recv_async().await;
    Ok(pair(
        written
            .unwrap_or(Err(PROCESS_EXITED_ERR.into()))
            .map(|()| true),
    ))
}

/// Wait until the process has exited, its output has ended and `on_exit`
/// has returned, and return the completed table. After {timeout} ms the
/// process gets SIGKILL and its code reads 124. Raises inside a `stdout` or
/// `stderr` callback of the same process.
///
/// @param timeout integer? Milliseconds to wait before killing the process.
/// @return (table?, string?) The completed table, or nil plus an error string.
/// @example
/// local done = assert(obj:wait(60000))
/// if done.code ~= 0 then print(done.stderr) end
#[lua_fn]
async fn wait(lua: Lua, this: UserDataRef<System>, timeout: Option<u64>) -> LuaResult<Pair<Table>> {
    let state = Arc::clone(&this.0);
    drop(this);
    if state.in_driver_task(&lua) {
        let completed = state.completed().clone();
        let completed = completed.ok_or_else(|| mlua::Error::runtime(WAIT_IN_CALLBACK_ERR))?;
        return Ok((Some(completed.to_table(&lua)?), None));
    }
    let limit = timeout.map(Duration::from_millis);
    if !state.finished_within(limit).await {
        state.timed_out.store(true, Ordering::Release);
        state.signal(SIGKILL);
        state.finished_within(limit).await;
    }
    let completed = try_pair!(state.completed().clone().ok_or(NOT_REAPED_ERR));
    Ok((Some(completed.to_table(&lua)?), None))
}

/// Send {signal} to the process group, unless the process was reaped.
///
/// @param signal integer? Signal number. Default 15 (SIGTERM).
/// @return
/// @example
/// obj:kill(9)
#[lua_fn]
fn kill(_lua: &Lua, this: &System, signal: Option<i32>) -> LuaResult<()> {
    this.0.signal(signal.unwrap_or(SIGTERM));
    Ok(())
}

/// Whether the process has exited and its output has ended.
///
/// @return (boolean)
/// @example
/// if not server:is_closing() then server:kill() end
#[lua_fn]
fn is_closing(_lua: &Lua, this: &System) -> LuaResult<bool> {
    Ok(this.0.completed().is_some())
}

fn system_fields<F: UserDataFields<System>>(fields: &mut F) {
    fields.add_field_method_get("pid", |_, this| Ok(this.0.pid));
}

lua_class! {
    /// A process started by `maki.system`, with field `pid` (integer).
    /// `write` and `wait` yield until done and can run at the same time.
    "maki.SystemObj" => System, SYSTEM_DOCS [write, wait, kill, is_closing] fields system_fields
}

#[cfg(all(test, unix))]
mod tests {
    use mlua::FromLua;
    use smol::LocalExecutor;
    use test_case::test_case;

    use super::*;
    use crate::api::util::process::CWD_NOT_DIR_ERR;
    use crate::runtime::block_on_or_fail;

    const TEST_PLUGIN: &str = "system_test";
    const BAD_CWD: &str = "/nonexistent/maki-system-test";
    const MISSING_PROGRAM: &str = "maki-system-test-no-such-program";
    const WAIT_TIMEOUT_MS: u64 = 200;

    /// Runs {code} with a `system` global that runs each driver next to the
    /// code, in place of the spawn queue, and an `unload` global that drops
    /// the drivers and kills the processes.
    fn run<R: FromLua + 'static>(code: &str) -> R {
        let lua = Lua::new();
        let (tx, drivers) = flume::unbounded();
        let tasks = Arc::new(Mutex::new(Vec::new()));
        let running = Arc::clone(&tasks);
        let system = move |lua: &Lua, (cmd, opts, on_exit)| {
            let (started, err) = start(lua, Arc::from(TEST_PLUGIN), cmd, opts, on_exit)?;
            let system = started.map(|(system, driver)| {
                tx.send(driver).expect("test holds the receiver");
                system
            });
            Ok((system, err))
        };
        let unload = move |_: &Lua, ()| {
            lock(&running).clear();
            kill_plugin_systems(TEST_PLUGIN);
            Ok(())
        };
        let globals = lua.globals();
        globals
            .set("system", lua.create_function(system).unwrap())
            .unwrap();
        globals
            .set("unload", lua.create_function(unload).unwrap())
            .unwrap();
        let main = lua.load(code).into_function().unwrap();
        let ex = LocalExecutor::new();
        let spawn_drivers = async {
            while let Ok(driver) = drivers.recv_async().await {
                let driver: Function = driver;
                lock(&tasks)
                    .push(ex.spawn(async move { driver.call_async::<()>(()).await.unwrap() }));
            }
            pending().await
        };
        block_on_or_fail(ex.run(or(main.call_async::<R>(()), spawn_drivers))).unwrap()
    }

    /// `cat` only exits once stdin closes, and only the callback closes it,
    /// so this hangs if output were held back until the process ends.
    #[test]
    fn stdout_callback_streams_while_the_process_runs() {
        let out: String = run(r#"
            local chunks, eof, obj = {}, false, nil
            obj = assert(system({ "cat" }, {
                stdin = true,
                stdout = function(err, data)
                    assert(err == nil)
                    if not data then eof = true return end
                    if #chunks == 0 then assert(obj:write(nil)) end
                    chunks[#chunks + 1] = data
                end,
            }))
            assert(obj.pid > 0)
            assert(obj:write("ping"))
            local done = obj:wait()
            return table.concat(chunks) .. "|" .. tostring(eof) .. "|" .. tostring(done.stdout) .. "|" .. done.code
            "#);
        assert_eq!(out, "ping|true|nil|0");
    }

    #[test_case(r#"{ "cat" }, { stdin = "abc" }"#, "abc|" ; "stdin_string_written_then_closed")]
    #[test_case(r#"{ "sh", "-c", "printf out; printf err >&2" }"#, "out|err" ; "collected_by_default")]
    #[test_case(r#"{ "sh", "-c", "printf out; printf err >&2" }, { stdout = false }"#, "nil|err" ; "stdout_discarded")]
    #[test_case(r#"{ "sh", "-c", "printf out; printf err >&2" }, { stderr = function() end }"#, "out|nil" ; "stderr_to_callback")]
    fn collects_output_into_completed_table(args: &str, expected: &str) {
        let out: String = run(&format!(
            r#"
            local done = system({args}):wait()
            return tostring(done.stdout) .. "|" .. tostring(done.stderr)
            "#
        ));
        assert_eq!(out, expected);
    }

    #[test_case(r#"{ "sh", "-c", "exit 3" }"#, "", "3|0" ; "exit_code")]
    #[test_case(r#"{ "sleep", "30" }"#, "obj:kill()", &format!("0|{SIGTERM}") ; "kill_defaults_to_sigterm")]
    #[test_case(r#"{ "sleep", "30" }"#, &format!("obj:kill({SIGKILL})"), &format!("0|{SIGKILL}") ; "kill_with_number")]
    #[test_case(r#"{ "sleep", "30" }"#, &format!("obj:wait({WAIT_TIMEOUT_MS})"), &format!("{TIMEOUT_EXIT_CODE}|{SIGKILL}") ; "wait_timeout_kills")]
    fn completed_reaches_wait_and_on_exit(cmd: &str, action: &str, expected: &str) {
        let out: String = run(&format!(
            r#"
            local seen
            local obj = system({cmd}, {{}}, function(c) seen = c end)
            {action}
            local done = obj:wait()
            assert(obj:is_closing() and seen.code == done.code)
            return done.code .. "|" .. done.signal
            "#
        ));
        assert_eq!(out, expected);
    }

    #[test_case("obj:kill() obj:wait()" ; "after_exit")]
    #[test_case("obj:write(nil)" ; "after_close")]
    fn write_refused(prelude: &str) {
        let out: String = run(&format!(
            r#"
            local obj = system({{ "cat" }}, {{ stdin = true }})
            {prelude}
            local ok, err = obj:write("x")
            obj:kill()
            return tostring(ok) .. "|" .. err
            "#
        ));
        assert_eq!(out, format!("nil|{STDIN_CLOSED_ERR}"));
    }

    /// The driver is running when the unload drops it, as the runtime does,
    /// so the handle settles without a reap and `on_exit` never runs.
    #[test]
    fn plugin_unload_kills_and_settles() {
        let out: String = run(r#"
            local exited = false
            local obj = system({ "cat" }, { stdin = true }, function() exited = true end)
            assert(obj:write("x"))
            unload()
            local done = obj:wait()
            local _, err = obj:write("y")
            return done.code .. "|" .. done.signal .. "|" .. tostring(obj:is_closing() and not exited) .. "|" .. err
            "#);
        assert_eq!(out, format!("0|{SIGKILL}|true|{STDIN_CLOSED_ERR}"));
    }

    #[test]
    fn empty_cmd_raises() {
        let out: String = run(r#"return tostring(select(2, pcall(system, {})))"#);
        assert!(out.contains(EMPTY_CMD_ERR), "{out}");
    }

    #[test_case(&format!(r#"{{ "true" }}, {{ cwd = "{BAD_CWD}" }}"#), CWD_NOT_DIR_ERR ; "bad_cwd")]
    #[test_case(&format!(r#"{{ "{MISSING_PROGRAM}" }}"#), CANNOT_START_ERR ; "missing_program")]
    fn start_failure_returns_nil_and_error(args: &str, prefix: &str) {
        let out: String = run(&format!(
            r#"
            local obj, err = system({args})
            return tostring(obj) .. "|" .. err
            "#
        ));
        assert!(out.starts_with(&format!("nil|{prefix}")), "{out}");
    }
}
