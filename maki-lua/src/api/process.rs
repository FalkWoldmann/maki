//! A child process a plugin talks to over its stdin and stdout, for servers
//! that speak a protocol there (a language server, a REPL).

use std::cell::RefCell;
use std::collections::HashMap;
use std::process::Command;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};

use futures_lite::io::{AsyncReadExt, AsyncWriteExt};
use maki_agent::ChildGuard;
use maki_lua_macro::{lua_class, lua_fn};
use maki_providers::strip_provider_keys;
use mlua::{Lua, LuaString, Result as LuaResult, Table, UserDataRef};
use smol::lock::Mutex as AsyncMutex;
use smol::process::{ChildStdin, ChildStdout, Stdio};

use crate::api::fs::expand_tilde;
use crate::api::util::pair::{Pair, err_pair, try_pair};

/// Anything past this waits in the pipe, not in our memory, so a plugin that
/// stops reading stalls the child instead of growing a buffer.
const READ_CHUNK: usize = 64 * 1024;
const READ_IN_PROGRESS: &str = "read already in progress";
const PROCESS_KILLED: &str = "process killed";
const EMPTY_ARGV_ERR: &str = "spawn: argv must not be empty";

thread_local! {
    static PLUGIN_PROCESSES: RefCell<HashMap<Arc<str>, Vec<Weak<ProcessState>>>> = RefCell::default();
}

/// A read or write in flight holds its own `Arc`, so the Lua handle can be
/// collected under it without killing the child mid-call.
struct ProcessState {
    /// Taken on kill. Dropping the guard kills the child's whole process
    /// group, which takes along whatever it started (a language server's
    /// `cargo check`, say).
    child: Mutex<Option<ChildGuard>>,
    killed: AtomicBool,
    /// Two readers of one pipe would each get a random slice of it, so a
    /// second read is refused rather than queued.
    stdout: AsyncMutex<(ChildStdout, Vec<u8>)>,
    /// A tool handler and a spawned task may share the process. Writes wait
    /// their turn here, so each one goes out whole and in call order.
    stdin: AsyncMutex<ChildStdin>,
}

impl ProcessState {
    fn kill(&self) {
        self.killed.store(true, Ordering::Release);
        let guard = self.child.lock().ok().and_then(|mut child| child.take());
        drop(guard);
    }

    fn ensure_alive(&self) -> Result<(), &'static str> {
        if self.killed.load(Ordering::Acquire) {
            return Err(PROCESS_KILLED);
        }
        Ok(())
    }
}

/// A write that stops partway leaves half a message in the pipe, and the
/// child would read the next write as the rest of it. So the process dies
/// unless the write finished.
struct TornWrite<'a>(Option<&'a ProcessState>);

impl Drop for TornWrite<'_> {
    fn drop(&mut self) {
        if let Some(process) = self.0 {
            process.kill();
        }
    }
}

pub(crate) struct Process(Arc<ProcessState>);

impl Process {
    fn track(plugin: Arc<str>, state: ProcessState) -> Self {
        let state = Arc::new(state);
        PLUGIN_PROCESSES.with_borrow_mut(|processes| {
            let running = processes.entry(plugin).or_default();
            running.retain(|process| process.strong_count() > 0);
            running.push(Arc::downgrade(&state));
        });
        Self(state)
    }
}

pub(crate) fn kill_plugin_processes(plugin: &str) {
    let processes = PLUGIN_PROCESSES.with_borrow_mut(|processes| processes.remove(plugin));
    for process in processes.iter().flatten().filter_map(Weak::upgrade) {
        process.kill();
    }
}

fn start(argv: &[String], opts: Option<&Table>) -> Result<ProcessState, String> {
    let (program, args) = argv.split_first().ok_or(EMPTY_ARGV_ERR)?;
    let mut std_cmd = Command::new(program);
    strip_provider_keys(&mut std_cmd).args(args);
    if let Some(opts) = opts {
        if let Ok(cwd) = opts.get::<String>("cwd") {
            let dir = expand_tilde(&cwd);
            if !dir.is_dir() {
                return Err(format!("cwd is not a directory: {}", dir.display()));
            }
            std_cmd.current_dir(dir);
        }
        if let Ok(env) = opts.get::<Table>("env") {
            std_cmd.envs(env.pairs::<String, String>().filter_map(Result::ok));
        }
    }

    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        // SAFETY: setsid is async-signal-safe, so it is sound to call in pre_exec.
        unsafe {
            std_cmd.pre_exec(|| {
                rustix::process::setsid()?;
                Ok(())
            });
        }
    }

    let mut cmd: smol::process::Command = std_cmd.into();
    cmd.stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null());
    let mut child = cmd
        .spawn()
        .map_err(|e| format!("cannot start {program}: {e}"))?;
    let (Some(stdin), Some(stdout)) = (child.stdin.take(), child.stdout.take()) else {
        return Err(format!("cannot start {program}: no stdio pipes"));
    };
    Ok(ProcessState {
        child: Mutex::new(Some(ChildGuard::new(child))),
        killed: AtomicBool::new(false),
        stdout: AsyncMutex::new((stdout, Vec::new())),
        stdin: AsyncMutex::new(stdin),
    })
}

/// Start {argv} with its stdin and stdout piped to you, for a child that
/// speaks a protocol there, such as a language server. Its stderr is
/// discarded. For a command whose output you read line by line, use
/// `jobstart`.
///
/// `read` and `write` yield, so a child that stays up belongs in a
/// `maki.async.spawn` task. It is killed, together with any process it
/// started, on `proc:kill()`, when the handle is garbage collected, and
/// when the plugin unloads.
///
/// {opts} fields:
///   `cwd` (string) Working directory (tilde is expanded).
///   `env` (table) Extra environment variables, `{ VAR = "value" }`.
///
/// @param argv table Program and arguments, like `{ "rust-analyzer" }`. No shell is involved.
/// @param opts table? Options (see above).
/// @return (maki.fn.Process?, string?) The process, or nil plus an error string.
/// @example
/// maki.async.spawn(function()
///   local proc, err = maki.fn.spawn({ "cat" })
///   if not proc then return maki.log.error(err) end
///   proc:write("hello\n")
///   print(proc:read()) -- hello
///   proc:kill()
/// end)
#[lua_fn(guard = Run)]
fn spawn(
    _lua: &Lua,
    #[ctx] plugin: Arc<str>,
    argv: Vec<String>,
    opts: Option<Table>,
) -> LuaResult<Pair<Process>> {
    let state = try_pair!(start(&argv, opts.as_ref()));
    Ok((Some(Process::track(plugin, state)), None))
}

/// Wait for output and return what arrived, at most 64 KiB. Returns
/// `nil, nil` once the child has closed its stdout.
///
/// Only one read at a time: a second `read()` while one is waiting returns
/// an error. A read and a write can run at the same time.
///
/// @return (string?, string?) Bytes read, or nil plus an error string, or nil, nil at end of stream.
/// @example
/// local chunk, err = proc:read()
/// if err then return maki.log.error(err) end
/// if not chunk then print("process exited") end
#[lua_fn]
async fn read(lua: Lua, this: UserDataRef<Process>) -> LuaResult<Pair<LuaString>> {
    let process = Arc::clone(&this.0);
    drop(this);
    let Some(mut stdout) = process.stdout.try_lock() else {
        return Ok(err_pair(READ_IN_PROGRESS));
    };
    try_pair!(process.ensure_alive());
    let (pipe, buf) = &mut *stdout;
    buf.resize(READ_CHUNK, 0);
    let read = pipe.read(&mut buf[..]).await;
    try_pair!(process.ensure_alive());
    match read {
        Ok(0) => Ok((None, None)),
        Ok(n) => Ok((Some(lua.create_string(&buf[..n])?), None)),
        Err(e) => Ok(err_pair(format!("read failed: {e}"))),
    }
}

/// Send {data} to the child's stdin and wait until all of it is written.
/// Writes made while one is in flight wait their turn, so each goes out
/// whole and in call order. A write that is cancelled or fails partway
/// kills the process, because the child would read the next write as the
/// rest of the cut one.
///
/// @param data string Bytes to send.
/// @return (boolean?, string?) `true`, or nil plus an error string.
/// @example
/// local ok, err = proc:write(maki.json.encode(msg) .. "\n")
/// if not ok then return maki.log.error(err) end
#[lua_fn]
async fn write(_lua: Lua, this: UserDataRef<Process>, data: LuaString) -> LuaResult<Pair<bool>> {
    let process = Arc::clone(&this.0);
    drop(this);
    let data = data.as_bytes().to_vec();
    let mut stdin = process.stdin.lock().await;
    try_pair!(process.ensure_alive());
    let mut torn = TornWrite(Some(&process));
    let written = async {
        stdin.write_all(&data).await?;
        stdin.flush().await
    }
    .await;
    match written {
        Ok(()) => {
            torn.0 = None;
            Ok((Some(true), None))
        }
        Err(_) if process.ensure_alive().is_err() => Ok(err_pair(PROCESS_KILLED)),
        Err(e) => Ok(err_pair(format!("write failed: {e}"))),
    }
}

/// Kill the process and any process it started. A read in flight ends
/// with `nil, nil` or an error. Extra calls do nothing.
///
/// @return
#[lua_fn]
fn kill(_lua: &Lua, this: &Process) -> LuaResult<()> {
    this.0.kill();
    Ok(())
}

lua_class! {
    /// A child process started by `maki.fn.spawn`.
    ///
    /// `read` and `write` yield until done and can run at the same time.
    /// The process is killed on `:kill()`, when the handle is garbage
    /// collected, and when the plugin unloads.
    "maki.fn.Process" => Process, PROCESS_DOCS [read, write, kill]
}

#[cfg(all(test, unix))]
mod tests {
    use mlua::{Function, Lua, Value};
    use test_case::test_case;

    use super::*;

    const TEST_PLUGIN: &str = "process_test";

    fn lua_with_spawn() -> Lua {
        let lua = Lua::new();
        let plugin: Arc<str> = Arc::from(TEST_PLUGIN);
        let spawn = lua
            .create_function(move |lua, (argv, opts): (Vec<String>, Option<Table>)| {
                let state = start(&argv, opts.as_ref()).map_err(mlua::Error::runtime)?;
                lua.create_userdata(Process::track(Arc::clone(&plugin), state))
            })
            .unwrap();
        lua.globals().set("spawn", spawn).unwrap();
        lua
    }

    fn run(lua: &Lua, code: &str) -> Value {
        let f: Function = lua.load(code).into_function().unwrap();
        smol::block_on(f.call_async(())).unwrap()
    }

    #[test]
    fn round_trips_bytes_through_stdin_and_stdout() {
        const FRAME: &str = "Content-Length: 2\r\n\r\n{}";
        let lua = lua_with_spawn();
        lua.globals().set("frame", FRAME).unwrap();
        let out = run(
            &lua,
            r#"
            local proc = spawn({ "cat" })
            assert(proc:write(frame))
            local got = ""
            while #got < #frame do got = got .. assert(proc:read()) end
            proc:kill()
            return got
            "#,
        );
        assert_eq!(out.to_string().unwrap(), FRAME);
    }

    #[test]
    fn read_returns_nil_nil_once_the_child_exits() {
        let lua = lua_with_spawn();
        let out = run(
            &lua,
            r#"
            local proc = spawn({ "true" })
            local chunk, err = proc:read()
            return chunk == nil and err == nil
            "#,
        );
        assert_eq!(out, Value::Boolean(true));
    }

    #[test_case("read()" ; "read")]
    #[test_case("write(\"x\")" ; "write")]
    fn a_killed_process_refuses_io(call: &str) {
        let lua = lua_with_spawn();
        let out = run(
            &lua,
            &format!(
                r#"
                local proc = spawn({{ "cat" }})
                proc:kill()
                local _, err = proc:{call}
                return err
                "#
            ),
        );
        assert_eq!(out.to_string().unwrap(), PROCESS_KILLED);
    }

    #[test]
    fn plugin_unload_kills_its_processes() {
        let lua = lua_with_spawn();
        lua.load(r#"proc = spawn({ "cat" })"#).exec().unwrap();
        kill_plugin_processes(TEST_PLUGIN);
        let out = run(&lua, r#"local _, err = proc:write("x"); return err"#);
        assert_eq!(out.to_string().unwrap(), PROCESS_KILLED);
    }

    #[test]
    fn empty_argv_is_refused() {
        assert_eq!(start(&[], None).err().as_deref(), Some(EMPTY_ARGV_ERR));
    }
}
