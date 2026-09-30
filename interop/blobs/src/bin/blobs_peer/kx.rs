//! go-iroh-key-exchange's request/report exchange, with the messages encoded
//! by the postcard crate from serde definitions of the same structs, and the
//! key-exchange groups chosen through rustls the way iroh's own
//! pq-only-key-exchange and prefer-pq-key-exchange examples do.
//!
//! Policies:
//!
//!   default    iroh's stock provider (ring): classical groups only
//!   classical  aws-lc-rs offering X25519 alone
//!   pq-only    aws-lc-rs offering X25519MLKEM768 alone
//!
//! Rust iroh does not report the negotiated group, so a Rust listener reports
//! the one group its policy offers; it has no default policy.

use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::Context as _;
use iroh::{Endpoint, EndpointAddr, EndpointId, TransportAddr, endpoint::presets};
use rustls::crypto::{CryptoProvider, aws_lc_rs};
use serde::{Deserialize, Serialize};

const ALPN: &[u8] = b"go-iroh-examples/key-exchange/1";

/// Request is the Go example's request struct.
#[derive(Debug, Serialize, Deserialize)]
struct Request {
    nonce: u64,
}

/// Report is the Go example's report struct.
#[derive(Debug, Serialize, Deserialize)]
struct Report {
    nonce: u64,
    group: String,
}

/// vectors prints the postcard encoding of each message the Go test pins.
pub fn vectors() -> anyhow::Result<()> {
    for nonce in [0, 0x5a5a, u64::MAX] {
        println!(
            "request {nonce:#x} {}",
            hex(&postcard::to_stdvec(&Request { nonce })?)
        );
    }
    for (nonce, group) in [
        (0, ""),
        (0x5a5a, "X25519"),
        (0x5a5a, "X25519MLKEM768"),
        (u64::MAX, "X25519MLKEM768"),
    ] {
        let b = postcard::to_stdvec(&Report {
            nonce,
            group: group.into(),
        })?;
        println!("report {nonce:#x} {group:?} {}", hex(&b));
    }
    Ok(())
}

/// provider returns the crypto provider for policy, or None for iroh's
/// default, and the one group it offers if there is exactly one.
fn provider(policy: &str) -> anyhow::Result<(Option<Arc<CryptoProvider>>, &'static str)> {
    let group = match policy {
        "default" => return Ok((None, "")),
        "classical" => aws_lc_rs::kx_group::X25519,
        "pq-only" => aws_lc_rs::kx_group::X25519MLKEM768,
        _ => anyhow::bail!("unknown policy {policy:?}"),
    };
    let mut p = aws_lc_rs::default_provider();
    p.kx_groups = vec![group];
    let name = match policy {
        "classical" => "X25519",
        _ => "X25519MLKEM768",
    };
    Ok((Some(Arc::new(p)), name))
}

async fn bind(
    policy: &str,
    addr: SocketAddr,
    alpns: Vec<Vec<u8>>,
) -> anyhow::Result<(Endpoint, &'static str)> {
    let (p, group) = provider(policy)?;
    let mut b = Endpoint::builder(presets::N0DisableRelay)
        .alpns(alpns)
        .bind_addr(addr)?;
    if let Some(p) = p {
        b = b.crypto_provider(p);
    }
    Ok((b.bind().await?, group))
}

/// listen answers one request the way the Go example's serve does: decode a
/// Request, reply with a Report carrying the nonce and the group.
pub async fn listen(policy: &str, addr: &str) -> anyhow::Result<()> {
    anyhow::ensure!(
        policy != "default",
        "a listener needs a single-group policy"
    );
    let addr: SocketAddr = addr.parse()?;
    let (ep, group) = bind(policy, addr, vec![ALPN.to_vec()]).await?;
    super::print_addr(&super::loopback_addr(&ep, addr)?);
    println!("READY");

    // A dialer with no group in common fails in the handshake; keep accepting
    // until one gets through or the test kills us.
    loop {
        let Some(incoming) = ep.accept().await else {
            return Ok(());
        };
        let Ok(conn) = async { incoming.accept()?.await.map_err(anyhow::Error::from) }.await else {
            continue;
        };
        let (mut send, mut recv) = conn.accept_bi().await?;
        let req: Request = postcard::from_bytes(&recv.read_to_end(1024).await?)?;
        let rep = Report {
            nonce: req.nonce,
            group: group.into(),
        };
        send.write_all(&postcard::to_stdvec(&rep)?).await?;
        send.finish()?;
        conn.closed().await;
        return Ok(());
    }
}

/// connect sends one Request and prints "REPORT <nonce> <group>" from the
/// Report that comes back, and "LOCAL <group>" when the policy fixes it.
pub async fn connect(policy: &str, id: &str, addr: &str, nonce: &str) -> anyhow::Result<()> {
    let id: EndpointId = id.parse()?;
    let sa: SocketAddr = addr.parse()?;
    let nonce: u64 = nonce.parse()?;
    let (ep, group) = bind(policy, super::dial_bind(sa), vec![]).await?;
    let conn = ep
        .connect(EndpointAddr::from_parts(id, [TransportAddr::Ip(sa)]), ALPN)
        .await
        .context("connect")?;
    let (mut send, mut recv) = conn.open_bi().await?;
    send.write_all(&postcard::to_stdvec(&Request { nonce })?)
        .await?;
    send.finish()?;
    let rep: Report = postcard::from_bytes(&recv.read_to_end(1024).await?)?;
    println!("REPORT {} {}", rep.nonce, rep.group);
    if !group.is_empty() {
        println!("LOCAL {group}");
    }
    conn.close(0u32.into(), b"bye");
    ep.close().await;
    Ok(())
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}
