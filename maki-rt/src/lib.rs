//! The process-wide tokio runtime.
//!
//! One current-thread runtime runs on its own thread: spawned tasks run there,
//! and it drives every socket and timer. Any other thread can block on a future
//! meanwhile, which is how sync code (the TUI loop, the Lua thread, tests)
//! reaches async code. A [`Task`] is cancelled when dropped unless detached.

use std::future::{Future, pending};
use std::panic::resume_unwind;
use std::pin::Pin;
use std::sync::LazyLock;
use std::task::{Context, Poll, ready};
use std::thread;

use tokio::runtime::{Builder, Handle};
use tokio::task::{JoinError, JoinHandle};

const RUNTIME_THREAD_NAME: &str = "maki-rt";

static RUNTIME: LazyLock<Handle> = LazyLock::new(|| {
    let runtime = Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("failed to build the tokio runtime");
    let handle = runtime.handle().clone();
    thread::Builder::new()
        .name(RUNTIME_THREAD_NAME.into())
        .spawn(move || runtime.block_on(pending::<()>()))
        .expect("failed to spawn the tokio runtime thread");
    handle
});

pub fn handle() -> &'static Handle {
    &RUNTIME
}

pub fn spawn<F>(future: F) -> Task<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    Task(Some(RUNTIME.spawn(future)))
}

/// Must run inside a [`tokio::task::LocalSet`].
pub fn spawn_local<F>(future: F) -> Task<F::Output>
where
    F: Future + 'static,
    F::Output: 'static,
{
    Task(Some(tokio::task::spawn_local(future)))
}

/// Panics when called from inside async code, like [`Handle::block_on`].
pub fn block_on<F: Future>(future: F) -> F::Output {
    RUNTIME.block_on(future)
}

/// The first of two futures to finish, preferring `a` when both are ready.
pub async fn or<T>(a: impl Future<Output = T>, b: impl Future<Output = T>) -> T {
    tokio::select! {
        biased;
        value = a => value,
        value = b => value,
    }
}

/// The first of two futures to finish, picking at random when both are ready.
pub async fn race<T>(a: impl Future<Output = T>, b: impl Future<Output = T>) -> T {
    tokio::select! {
        value = a => value,
        value = b => value,
    }
}

/// Runs `f` on the blocking pool, re-raising its panic here.
pub async fn unblock<T, F>(f: F) -> T
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    unwrap_join(RUNTIME.spawn_blocking(f).await)
}

fn unwrap_join<T>(result: Result<T, JoinError>) -> T {
    match result {
        Ok(value) => value,
        Err(e) if e.is_panic() => resume_unwind(e.into_panic()),
        Err(e) => panic!("task did not complete: {e}"),
    }
}

/// A spawned task that is aborted when dropped, unless [`Task::detach`]ed.
#[must_use = "dropping a Task cancels it; call detach() to let it run"]
pub struct Task<T>(Option<JoinHandle<T>>);

impl<T> Task<T> {
    pub fn detach(mut self) {
        self.0.take();
    }

    pub fn is_finished(&self) -> bool {
        self.0.as_ref().is_none_or(JoinHandle::is_finished)
    }

    /// Aborts the task and waits for it to stop. `None` when it had not
    /// finished yet.
    pub async fn cancel(mut self) -> Option<T> {
        let handle = self.0.take()?;
        handle.abort();
        match handle.await {
            Ok(value) => Some(value),
            Err(e) if e.is_cancelled() => None,
            Err(e) => resume_unwind(e.into_panic()),
        }
    }
}

impl<T> Drop for Task<T> {
    fn drop(&mut self) {
        if let Some(handle) = &self.0 {
            handle.abort();
        }
    }
}

impl<T> Future for Task<T> {
    type Output = T;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<T> {
        let handle = self.0.as_mut().expect("task polled after completion");
        let output = ready!(Pin::new(handle).poll(cx));
        self.0 = None;
        Poll::Ready(unwrap_join(output))
    }
}

#[cfg(test)]
mod tests {
    use tokio::sync::oneshot;

    use super::*;

    const PANIC_MESSAGE: &str = "boom";

    /// `closed()` resolves only once the task, and the receiver it owns, is dropped.
    #[test]
    fn dropping_a_task_cancels_it() {
        let (mut tx, rx) = oneshot::channel::<()>();
        drop(spawn(async move {
            let _ = rx.await;
        }));
        block_on(tx.closed());
    }

    #[test]
    fn a_detached_task_keeps_running() {
        let (tx, rx) = tokio::sync::oneshot::channel();
        spawn(async move { tx.send(()).unwrap() }).detach();
        block_on(rx).unwrap();
    }

    #[test]
    fn cancel_returns_none_for_an_unfinished_task() {
        let task = spawn(pending::<()>());
        assert_eq!(block_on(task.cancel()), None);
    }

    #[test]
    fn a_task_panic_resurfaces_at_the_await() {
        let task = spawn(async { panic!("{PANIC_MESSAGE}") });
        let payload = std::panic::catch_unwind(|| block_on(task)).unwrap_err();
        assert_eq!(payload.downcast_ref::<String>().unwrap(), PANIC_MESSAGE);
    }

    #[test]
    fn unblock_returns_the_closure_result() {
        assert_eq!(block_on(unblock(|| 2 + 2)), 4);
    }
}
