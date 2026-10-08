//! Spawning a tap or target executable, tolerating a transient `ETXTBSY`.
//!
//! On Linux, `execve` fails with `ETXTBSY` ("Text file busy") while any process
//! holds the executable open for writing. A script written just before it is
//! run can hit this through no fault of its own: a child forked concurrently by
//! another thread inherits the still-open write descriptor until it calls
//! `exec` itself. The window is short, so a few brief retries resolve it.

use std::io;
use std::time::Duration;

/// Attempts made before an `ETXTBSY` spawn failure is returned.
pub const SPAWN_ATTEMPTS: u32 = 5;

const FIRST_BACKOFF: Duration = Duration::from_millis(10);

/// Whether a spawn failure is the transient "text file busy" error.
pub fn is_text_file_busy(err: &io::Error) -> bool {
    err.kind() == io::ErrorKind::ExecutableFileBusy
}

/// Backoff before retry number `attempt` (1-based): 10, 20, 40, … ms.
pub fn spawn_backoff(attempt: u32) -> Duration {
    FIRST_BACKOFF * 2u32.saturating_pow(attempt.saturating_sub(1))
}

/// Run `spawn`, retrying after `sleep(spawn_backoff(n))` while it fails with
/// `ETXTBSY`, up to [`SPAWN_ATTEMPTS`] attempts. Any other error is returned
/// at once.
pub fn spawn_retrying<T>(
    mut spawn: impl FnMut() -> io::Result<T>,
    mut sleep: impl FnMut(Duration),
) -> io::Result<T> {
    let mut attempt = 1;
    loop {
        match spawn() {
            Err(e) if is_text_file_busy(&e) && attempt < SPAWN_ATTEMPTS => {
                sleep(spawn_backoff(attempt));
                attempt += 1;
            }
            other => return other,
        }
    }
}

/// Spawn `command`, retrying a transient `ETXTBSY` (see the module docs).
///
/// The wait between attempts blocks the calling thread; it only happens on the
/// `ETXTBSY` path and totals at most 150 ms.
pub fn spawn_command(command: &mut tokio::process::Command) -> io::Result<tokio::process::Child> {
    spawn_retrying(|| command.spawn(), std::thread::sleep)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;

    fn busy() -> io::Error {
        io::Error::from(io::ErrorKind::ExecutableFileBusy)
    }

    #[test]
    fn only_text_file_busy_is_transient() {
        assert!(is_text_file_busy(&busy()));
        assert!(!is_text_file_busy(&io::Error::from(
            io::ErrorKind::NotFound
        )));
        assert!(!is_text_file_busy(&io::Error::from(
            io::ErrorKind::PermissionDenied
        )));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn raw_etxtbsy_maps_to_text_file_busy() {
        assert!(is_text_file_busy(&io::Error::from_raw_os_error(26)));
    }

    #[test]
    fn backoff_doubles() {
        assert_eq!(spawn_backoff(1), Duration::from_millis(10));
        assert_eq!(spawn_backoff(2), Duration::from_millis(20));
        assert_eq!(spawn_backoff(4), Duration::from_millis(80));
    }

    #[test]
    fn retries_busy_then_succeeds() {
        let calls = RefCell::new(0);
        let slept = RefCell::new(Vec::new());
        let out = spawn_retrying(
            || {
                *calls.borrow_mut() += 1;
                if *calls.borrow() < 3 {
                    Err(busy())
                } else {
                    Ok(7)
                }
            },
            |d| slept.borrow_mut().push(d),
        )
        .unwrap();
        assert_eq!(out, 7);
        assert_eq!(*calls.borrow(), 3);
        assert_eq!(
            *slept.borrow(),
            vec![Duration::from_millis(10), Duration::from_millis(20)]
        );
    }

    #[test]
    fn other_errors_are_not_retried() {
        let calls = RefCell::new(0);
        let err = spawn_retrying::<()>(
            || {
                *calls.borrow_mut() += 1;
                Err(io::Error::from(io::ErrorKind::NotFound))
            },
            |_| {},
        )
        .unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::NotFound);
        assert_eq!(*calls.borrow(), 1);
    }

    #[test]
    fn gives_up_after_the_attempt_budget() {
        let calls = RefCell::new(0);
        let err = spawn_retrying::<()>(
            || {
                *calls.borrow_mut() += 1;
                Err(busy())
            },
            |_| {},
        )
        .unwrap_err();
        assert!(is_text_file_busy(&err));
        assert_eq!(*calls.borrow(), SPAWN_ATTEMPTS);
    }

    #[tokio::test]
    async fn spawns_a_real_command() {
        let program = if cfg!(windows) { "cmd" } else { "true" };
        let mut command = tokio::process::Command::new(program);
        if cfg!(windows) {
            command.args(["/C", "exit 0"]);
        }
        let mut child = spawn_command(&mut command).unwrap();
        assert!(child.wait().await.unwrap().success());
    }
}
