//! Every git operation maki performs, done in-process with gix.
//!
//! Each call names the repository it works on, so no operation can pick up the
//! config of whatever repository maki was launched from. Credential helpers,
//! SSH, and proxies still come from the user's git configuration, which gix
//! reads the same way the git binary does.

use std::fmt::Display;
use std::fs;
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::AtomicBool;

use gix::bstr::ByteSlice;
use gix::create;
use gix::object::tree::EntryKind;
use gix::progress::Discard;
use gix::refs::transaction::{Change, LogChange, PreviousValue, RefEdit};
use gix::refs::{FullName, Target};
use gix::remote::Direction;
use gix::remote::fetch::Tags;
use gix::sec::Trust;
use gix::sec::trust::DefaultForLevel;
use gix::traverse::tree::Recorder;

/// Peels whatever a ref names down to a commit.
const COMMIT_PEEL: &str = "^{commit}";
const TREE_PEEL: &str = "^{tree}";
const ORIGIN: &str = "origin";
const REMOTE_PREFIX: &str = "refs/remotes/origin/";
const REMOTE_HEAD: &str = "refs/remotes/origin/HEAD";
const GIT_DIR: &str = ".git";
#[cfg(unix)]
const EXECUTABLE_MODE: u32 = 0o755;

/// Config every repository is opened with, outranking every config file.
///
/// `ext::` hands its argument to a shell, and it is what a hostile
/// `url.<base>.insteadOf` reaches for. gix refuses it by default, but that
/// default sits in config a repository can override. The speed limits turn a
/// remote that connects and then goes quiet into a failure instead of a wait
/// with no end. Hooks need no setting: gix never runs them.
const HARDENING: [&str; 3] = [
    "protocol.ext.allow=never",
    "http.lowSpeedLimit=1",
    "http.lowSpeedTime=30",
];

#[derive(Debug, thiserror::Error)]
pub enum GitError {
    #[error("git {op} failed for {target}: {message}")]
    Failed {
        op: &'static str,
        target: String,
        message: String,
    },
}

/// Whether a revision is usable at all. Revisions come from a package
/// specification, so one that looks like an option is refused rather than
/// looked up.
pub fn revision_is_safe(rev: &str) -> bool {
    !rev.is_empty() && !rev.starts_with('-')
}

/// Whether an HTTP source embeds user information that git would persist.
///
/// Tokens in clone URLs are commonly written as a username, a password, or
/// both. The URL lands in the clone's config, and maki also records the source
/// in its lockfile. Reject that form and let git's credential helper provide
/// credentials without putting them on disk as part of the repository URL.
pub fn http_source_has_userinfo(src: &str) -> bool {
    let Some((scheme, rest)) = src.trim().split_once("://") else {
        return false;
    };
    if !scheme.eq_ignore_ascii_case("http") && !scheme.eq_ignore_ascii_case("https") {
        return false;
    }
    let authority = rest.split(['/', '?', '#']).next().unwrap_or(rest);
    authority.contains('@')
}

/// Whether a clone's recorded origin is the source being asked for. gix
/// records a local path fully resolved, so two spellings of one directory
/// (`/var` and `/private/var` on macOS) still count as the same source.
pub fn same_source(recorded: &str, requested: &str) -> bool {
    let (recorded, requested) = (recorded.trim(), requested.trim());
    recorded == requested
        || Path::new(requested)
            .canonicalize()
            .is_ok_and(|requested| Path::new(recorded) == requested)
}

/// Hides credentials embedded in a URL, so an error message can be shown or
/// logged without leaking a token that was only ever meant for git.
pub fn redact(text: &str) -> String {
    text.split_inclusive(char::is_whitespace)
        .map(|part| {
            let word = part.trim_end_matches(char::is_whitespace);
            let whitespace = &part[word.len()..];
            let redacted = match word.split_once("://") {
                Some((scheme, rest)) => match rest.rsplit_once('@') {
                    Some((_creds, host)) => format!("{scheme}://***@{host}"),
                    None => word.to_owned(),
                },
                None => word.to_owned(),
            };
            redacted + whitespace
        })
        .collect()
}

fn failed(op: &'static str, target: impl Display, error: impl Display) -> GitError {
    GitError::Failed {
        op,
        target: redact(&target.to_string()),
        message: redact(&error.to_string()),
    }
}

/// Full trust, plus the git installation's own config so credential helpers
/// and proxies keep working, with [`HARDENING`] on top of all of it.
fn open_options() -> gix::open::Options {
    let mut options =
        gix::open::Options::default_for_level(Trust::Full).config_overrides(HARDENING);
    options.permissions.config.git_binary = true;
    options
}

fn open(work: &Path) -> Result<gix::Repository, GitError> {
    gix::open_opts(work, open_options()).map_err(|error| failed("open", work.display(), error))
}

/// gix blocks, and `maki.pack.add` runs on the single Lua thread while
/// `init.lua` is sourced, so every operation runs off it.
async fn off_thread<T: Send + 'static>(
    job: impl FnOnce() -> Result<T, GitError> + Send + 'static,
) -> Result<T, GitError> {
    smol::unblock(job).await
}

pub fn is_repository(work: &Path) -> bool {
    open(work).is_ok()
}

/// A bare clone: revisions are exported from its objects, so nothing ever
/// checks out a working tree that a session could read mid-change.
pub async fn clone(src: String, dest: PathBuf) -> Result<(), GitError> {
    off_thread(move || {
        let fail = |error: &dyn Display| failed("clone", &src, error);
        let mut prepare = gix::clone::PrepareFetch::new(
            src.as_str(),
            &dest,
            create::Kind::Bare,
            create::Options::default(),
            open_options(),
        )
        .map_err(|error| fail(&error))?;
        let (repo, _) = prepare
            .fetch_only(Discard, &AtomicBool::new(false))
            .map_err(|error| fail(&error))?;
        track_remote_head(&repo).map_err(|error| fail(&error))
    })
    .await
}

/// gix records `origin/HEAD` as the commit it saw, where git makes it a
/// symbolic ref to the default branch. Only the symbolic form moves with a
/// fetch, and the default branch resolves through it.
fn track_remote_head(
    repo: &gix::Repository,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let Some(branch) = repo.head_name()? else {
        return Ok(());
    };
    let target: FullName = format!("{REMOTE_PREFIX}{}", branch.shorten()).try_into()?;
    repo.edit_reference(RefEdit {
        change: Change::Update {
            log: LogChange::default(),
            expected: PreviousValue::Any,
            new: Target::Symbolic(target),
        },
        name: REMOTE_HEAD.try_into()?,
        deref: false,
    })?;
    Ok(())
}

/// Fetches every tag too, since a version names a tag as often as a branch.
pub async fn fetch(work: PathBuf) -> Result<(), GitError> {
    off_thread(move || {
        let fail = |error: &dyn Display| failed("fetch", work.display(), error);
        let repo = open(&work)?;
        let remote = repo
            .find_remote(ORIGIN)
            .map_err(|error| fail(&error))?
            .with_fetch_tags(Tags::All);
        remote
            .connect(Direction::Fetch)
            .map_err(|error| fail(&error))?
            .prepare_fetch(Discard, Default::default())
            .map_err(|error| fail(&error))?
            .receive(Discard, &AtomicBool::new(false))
            .map_err(|error| fail(&error))?;
        Ok(())
    })
    .await
}

/// Resolves a ref to a commit id. An annotated tag is an object of its own, so
/// an unpeeled lookup would record the tag's id in the lockfile rather than the
/// commit that lockfile is supposed to reproduce.
pub async fn resolve_commit(work: PathBuf, rev: String) -> Result<String, GitError> {
    off_thread(move || {
        let repo = open(&work)?;
        repo.rev_parse_single(format!("{rev}{COMMIT_PEEL}").as_str())
            .map(|id| id.to_string())
            .map_err(|error| failed("rev-parse", &rev, error))
    })
    .await
}

pub async fn origin_url(work: PathBuf) -> Result<String, GitError> {
    off_thread(move || {
        let repo = open(&work)?;
        let remote = repo
            .find_remote(ORIGIN)
            .map_err(|error| failed("remote", work.display(), error))?;
        Ok(remote
            .url(Direction::Fetch)
            .map(|url| url.to_bstring().to_string())
            .unwrap_or_default())
    })
    .await
}

fn tree_at<'repo>(repo: &'repo gix::Repository, rev: &str) -> Result<gix::Tree<'repo>, GitError> {
    let fail = |error: &dyn Display| failed("rev-parse", rev, error);
    Ok(repo
        .rev_parse_single(format!("{rev}{TREE_PEEL}").as_str())
        .map_err(|error| fail(&error))?
        .object()
        .map_err(|error| fail(&error))?
        .into_tree())
}

/// The file at `path` in `rev`, or `None` when the revision has no such file.
pub async fn read_file(
    work: PathBuf,
    rev: String,
    path: &'static str,
) -> Result<Option<String>, GitError> {
    off_thread(move || {
        let fail = |error: &dyn Display| failed("show", format!("{rev}:{path}"), error);
        let repo = open(&work)?;
        let Some(entry) = tree_at(&repo, &rev)?
            .lookup_entry_by_path(path)
            .map_err(|error| fail(&error))?
        else {
            return Ok(None);
        };
        let blob = entry.object().map_err(|error| fail(&error))?;
        Ok(Some(blob.data.to_str_lossy().into_owned()))
    })
    .await
}

/// A tree path is written below the export root only if every component is a
/// plain name. A crafted tree can hold `..` or `.git`, which git itself refuses
/// to check out.
fn entry_path_is_safe(path: &Path) -> bool {
    path.components().all(|component| match component {
        Component::Normal(name) => !name.eq_ignore_ascii_case(GIT_DIR),
        _ => false,
    })
}

/// Writes the files of `rev` below `dest`, symlinks included as they are
/// committed. Whether a symlink may stay is the caller's policy. Submodules are
/// not part of a package and are skipped.
pub async fn export_tree(work: PathBuf, rev: String, dest: PathBuf) -> Result<(), GitError> {
    off_thread(move || {
        let fail = |error: &dyn Display| failed("export", format!("{rev} -> {}", dest.display()), error);
        let repo = open(&work)?;
        let mut recorder = Recorder::default();
        tree_at(&repo, &rev)?
            .traverse()
            .breadthfirst(&mut recorder)
            .map_err(|error| fail(&error))?;
        fs::create_dir_all(&dest).map_err(|error| fail(&error))?;
        for entry in recorder.records {
            let relative = gix::path::from_bstr(entry.filepath.as_bstr());
            if !entry_path_is_safe(&relative) {
                tracing::warn!(path = %relative.display(), "package tree entry has an unsafe path; it was not exported");
                continue;
            }
            let path = dest.join(relative);
            let kind = entry.mode.kind();
            match kind {
                EntryKind::Tree => fs::create_dir_all(&path).map_err(|error| fail(&error))?,
                EntryKind::Blob | EntryKind::BlobExecutable => {
                    let blob = repo.find_blob(entry.oid).map_err(|error| fail(&error))?;
                    fs::write(&path, &blob.data).map_err(|error| fail(&error))?;
                    #[cfg(unix)]
                    if kind == EntryKind::BlobExecutable {
                        use std::os::unix::fs::PermissionsExt;
                        fs::set_permissions(&path, fs::Permissions::from_mode(EXECUTABLE_MODE))
                            .map_err(|error| fail(&error))?;
                    }
                }
                EntryKind::Link => {
                    let blob = repo.find_blob(entry.oid).map_err(|error| fail(&error))?;
                    let target = gix::path::from_bstr(blob.data.as_bstr()).into_owned();
                    create_symlink(&target, &path).map_err(|error| fail(&error))?;
                }
                EntryKind::Commit => {}
            }
        }
        Ok(())
    })
    .await
}

#[cfg(unix)]
fn create_symlink(target: &Path, link: &Path) -> std::io::Result<()> {
    std::os::unix::fs::symlink(target, link)
}

#[cfg(windows)]
fn create_symlink(target: &Path, link: &Path) -> std::io::Result<()> {
    std::os::windows::fs::symlink_file(target, link)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::process::Command;
    use test_case::test_case;

    const EXT_SOURCE: &str = "ext::sh -c touch% pwned";
    const FILE_NAME: &str = "plugin.toml";
    const FILE_BODY: &str = "name = \"demo\"\n";
    const TAG: &str = "v1";

    fn git_cli(dir: &Path, args: &[&str]) -> String {
        let output = Command::new("git")
            .args(args)
            .current_dir(dir)
            .output()
            .unwrap();
        assert!(output.status.success(), "git {args:?}: {output:?}");
        String::from_utf8(output.stdout).unwrap().trim().to_owned()
    }

    /// An origin with one file, an executable, a symlink, and an annotated tag.
    fn origin(dir: &Path) -> PathBuf {
        let origin = dir.join("origin");
        fs::create_dir_all(origin.join("bin")).unwrap();
        fs::write(origin.join(FILE_NAME), FILE_BODY).unwrap();
        fs::write(origin.join("bin").join("run"), "#!/bin/sh\n").unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(
                origin.join("bin").join("run"),
                fs::Permissions::from_mode(EXECUTABLE_MODE),
            )
            .unwrap();
            std::os::unix::fs::symlink(FILE_NAME, origin.join("link")).unwrap();
        }
        for args in [
            &["init", "--quiet"][..],
            &["add", "."],
            &[
                "-c",
                "user.email=t@example.com",
                "-c",
                "user.name=t",
                "commit",
                "--quiet",
                "-m",
                "init",
            ],
            &[
                "-c",
                "user.email=t@example.com",
                "-c",
                "user.name=t",
                "tag",
                "-a",
                TAG,
                "-m",
                TAG,
            ],
        ] {
            git_cli(&origin, args);
        }
        origin
    }

    fn cloned(dir: &Path) -> (PathBuf, PathBuf) {
        let origin = origin(dir);
        let work = dir.join("work");
        smol::block_on(clone(origin.display().to_string(), work.clone())).unwrap();
        (origin, work)
    }

    /// The whole point of pinning `protocol.ext.allow`: no shell may run.
    #[test]
    fn the_ext_transport_is_refused() {
        let dir = tempfile::TempDir::new().unwrap();
        let result = smol::block_on(clone(EXT_SOURCE.to_owned(), dir.path().join("work")));
        assert!(result.is_err());
        assert!(!dir.path().join("pwned").exists());
    }

    #[test]
    fn an_annotated_tag_resolves_to_its_commit() {
        let dir = tempfile::TempDir::new().unwrap();
        let (origin, work) = cloned(dir.path());
        let head = git_cli(&origin, &["rev-parse", "HEAD"]);
        let tag = smol::block_on(resolve_commit(work, TAG.to_owned())).unwrap();
        assert_eq!(tag, head);
    }

    #[test]
    fn origin_url_is_the_cloned_source() {
        let dir = tempfile::TempDir::new().unwrap();
        let (origin, work) = cloned(dir.path());
        let url = smol::block_on(origin_url(work)).unwrap();
        assert!(same_source(&url, &origin.display().to_string()), "{url}");
        assert!(!same_source(&url, "https://example.com/other"));
    }

    #[test]
    fn a_fetch_brings_in_a_tag_pushed_later() {
        let dir = tempfile::TempDir::new().unwrap();
        let (origin, work) = cloned(dir.path());
        git_cli(&origin, &["tag", "later"]);
        assert!(smol::block_on(resolve_commit(work.clone(), "later".to_owned())).is_err());
        smol::block_on(fetch(work.clone())).unwrap();
        assert!(smol::block_on(resolve_commit(work, "later".to_owned())).is_ok());
    }

    #[test]
    fn read_file_distinguishes_missing_from_present() {
        let dir = tempfile::TempDir::new().unwrap();
        let (_origin, work) = cloned(dir.path());
        let found = smol::block_on(read_file(work.clone(), TAG.to_owned(), FILE_NAME)).unwrap();
        assert_eq!(found.as_deref(), Some(FILE_BODY));
        let missing = smol::block_on(read_file(work, TAG.to_owned(), "absent.toml")).unwrap();
        assert!(missing.is_none());
    }

    #[cfg(unix)]
    #[test]
    fn export_keeps_modes_and_symlinks() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::TempDir::new().unwrap();
        let (_origin, work) = cloned(dir.path());
        let dest = dir.path().join("export");
        smol::block_on(export_tree(work, TAG.to_owned(), dest.clone())).unwrap();
        assert_eq!(fs::read_to_string(dest.join(FILE_NAME)).unwrap(), FILE_BODY);
        let mode = fs::metadata(dest.join("bin").join("run"))
            .unwrap()
            .permissions()
            .mode();
        assert_eq!(mode & EXECUTABLE_MODE, EXECUTABLE_MODE);
        assert_eq!(
            fs::read_link(dest.join("link")).unwrap(),
            Path::new(FILE_NAME)
        );
    }

    #[test_case("plugin/init.lua", true ; "plain")]
    #[test_case("../escape", false ; "parent")]
    #[test_case(".git/hooks/post-checkout", false ; "git_dir")]
    #[test_case(".GIT/config", false ; "git_dir_case")]
    #[test_case("/etc/passwd", false ; "absolute")]
    fn tree_paths_are_checked(path: &str, expected: bool) {
        assert_eq!(entry_path_is_safe(Path::new(path)), expected);
    }

    /// A clone URL can carry a token. Errors are shown and logged, so the
    /// credential must not travel with them.
    #[test]
    fn credentials_in_a_url_are_redacted() {
        let out = redact("clone -- https://user:tok3n@github.com/u/r /dest");
        assert!(!out.contains("tok3n"), "token leaked: {out}");
        assert!(!out.contains("user:"), "user leaked: {out}");
        assert!(out.contains("github.com/u/r"), "host should survive: {out}");
        assert!(out.contains("/dest"));
    }

    #[test_case("https://token@example.com/repo", true ; "username_token")]
    #[test_case("https://user:token@example.com/repo", true ; "password_token")]
    #[test_case("HTTP://user@example.com/repo", true ; "scheme_case")]
    #[test_case("https://example.com/user@repo", false ; "at_in_path")]
    #[test_case("ssh://git@example.com/repo", false ; "ssh_username")]
    #[test_case("git@example.com:user/repo", false ; "scp_style")]
    fn detects_http_user_information(src: &str, expected: bool) {
        assert_eq!(http_source_has_userinfo(src), expected);
    }

    #[test]
    fn redaction_leaves_ordinary_urls_alone() {
        let plain = "clone -- https://github.com/u/r /dest";
        assert_eq!(redact(plain), plain);
    }

    #[test]
    fn redaction_preserves_whitespace() {
        let source = "/a path/with spaces\nand a second line";
        assert_eq!(redact(source), source);
    }

    #[test]
    fn revisions_that_look_like_options_are_rejected() {
        assert!(revision_is_safe("main"));
        assert!(revision_is_safe("v1.2.3"));
        assert!(revision_is_safe("abc123"));
        assert!(!revision_is_safe("--upload-pack=evil"));
        assert!(!revision_is_safe("-x"));
        assert!(!revision_is_safe(""));
    }
}
