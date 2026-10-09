//! Child processes in a process group of their own, so a signal reaches
//! everything they start.

use std::process::Command;

use maki_providers::strip_provider_keys;

use crate::api::fs::expand_tilde;

pub(crate) const SIGKILL: i32 = 9;
pub(crate) const SIGTERM: i32 = 15;
pub(crate) const CWD_NOT_DIR_ERR: &str = "cwd is not a directory";

/// Strips the provider keys, starts the child in a session of its own and
/// runs it in {cwd} (tilde expanded).
pub(crate) fn isolate(command: &mut Command, cwd: Option<&str>) -> Result<(), String> {
    strip_provider_keys(command);
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        // SAFETY: setsid is async-signal-safe, so it is sound to call in pre_exec.
        unsafe {
            command.pre_exec(|| {
                rustix::process::setsid()?;
                Ok(())
            });
        }
    }
    if let Some(dir) = cwd.map(expand_tilde) {
        if !dir.is_dir() {
            return Err(format!("{CWD_NOT_DIR_ERR}: {}", dir.display()));
        }
        command.current_dir(dir);
    }
    Ok(())
}

/// Sends {signal} to the process group led by {pid}. Windows has no
/// signals, so there the whole tree is killed.
pub(crate) fn signal_group(pid: u32, signal: i32) {
    #[cfg(unix)]
    {
        use rustix::process::{Pid, Signal, kill_process_group};
        if let Ok(raw) = i32::try_from(pid)
            && let Some(pid) = Pid::from_raw(raw)
            && let Some(signal) = Signal::from_named_raw(signal)
        {
            let _ = kill_process_group(pid, signal);
        }
    }
    #[cfg(windows)]
    {
        let _ = signal;
        let _ = Command::new("taskkill")
            .args(["/T", "/F", "/PID", &pid.to_string()])
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn();
    }
}
