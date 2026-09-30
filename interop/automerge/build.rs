//! Copies n0's iroh-automerge protocol module into OUT_DIR so the peer can
//! compile it.
//!
//! iroh-automerge is a binary crate, so cargo fetches it as a git dependency
//! but will not link it. Its source is in cargo's git checkout of
//! n0-computer/iroh-examples, at the commit Cargo.lock records; this script
//! finds that checkout and copies src/protocol.rs byte for byte. Nothing is
//! re-typed, and updating Cargo.lock is how the peer moves to a new upstream.

use std::{env, fs, path::PathBuf};

fn main() {
    println!("cargo:rerun-if-changed=Cargo.lock");
    let lock = fs::read_to_string("Cargo.lock").expect("read Cargo.lock");
    let rev = upstream_rev(&lock).expect("Cargo.lock has no git source for iroh-automerge");

    let cargo_home = env::var_os("CARGO_HOME")
        .map(PathBuf::from)
        .or_else(|| env::var_os("HOME").map(|h| PathBuf::from(h).join(".cargo")))
        .expect("CARGO_HOME or HOME must be set");
    let checkouts = cargo_home.join("git").join("checkouts");

    // Cargo names a checkout <repo>-<hash of url>/<first 7 hex digits of rev>.
    let short = &rev[..7];
    let src = fs::read_dir(&checkouts)
        .unwrap_or_else(|e| panic!("read {}: {e}", checkouts.display()))
        .filter_map(Result::ok)
        .filter(|d| {
            d.file_name()
                .to_string_lossy()
                .starts_with("iroh-examples-")
        })
        .map(|d| d.path().join(short).join("iroh-automerge/src/protocol.rs"))
        .find(|p| p.exists())
        .unwrap_or_else(|| {
            panic!(
                "no checkout of iroh-examples at {rev} under {}; run cargo fetch",
                checkouts.display()
            )
        });

    println!("cargo:rerun-if-changed={}", src.display());
    println!("cargo:rustc-env=IROH_AUTOMERGE_REV={rev}");
    let out = PathBuf::from(env::var_os("OUT_DIR").unwrap()).join("protocol.rs");
    fs::copy(&src, &out).unwrap_or_else(|e| panic!("copy {}: {e}", src.display()));
}

// upstream_rev returns the commit Cargo.lock pins iroh-automerge to.
fn upstream_rev(lock: &str) -> Option<String> {
    let mut in_pkg = false;
    for line in lock.lines() {
        if line == "[[package]]" {
            in_pkg = false;
        } else if line == "name = \"iroh-automerge\"" {
            in_pkg = true;
        } else if in_pkg {
            if let Some(src) = line.strip_prefix("source = \"") {
                let (_, rev) = src.trim_end_matches('"').rsplit_once('#')?;
                return Some(rev.to_string());
            }
        }
    }
    None
}
