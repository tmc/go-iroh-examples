//! A Rust iroh peer that syncs an Automerge document using n0's own
//! iroh-automerge protocol code, so go-iroh-automerge can be tested against
//! the implementation it claims to match rather than against itself.
//!
//!   automerge_peer listen                  print "ADDR <id> <ip:port>", respond to one sync
//!   automerge_peer connect <id> <ip:port>  dial that address and initiate a sync
//!   automerge_peer vectors                 print the ALPN and a sync message as hex
//!
//! Both live modes start from the same edits, disable relays and bind
//! 127.0.0.1, and after the sync print the whole document as "State" followed
//! by one `key => "value"` line per key in sorted order.

use std::{net::SocketAddr, sync::Arc, time::Duration};

use anyhow::Context as _;
use automerge::{
    Automerge, ReadDoc,
    sync::{self, SyncDoc},
    transaction::Transactable,
};
use iroh::{
    Endpoint, EndpointAddr, EndpointId, TransportAddr, endpoint::presets, protocol::Router,
};
use tokio::sync::mpsc;

/// Upstream's src/protocol.rs, copied unchanged by build.rs.
#[allow(dead_code)]
mod protocol {
    include!(concat!(env!("OUT_DIR"), "/protocol.rs"));
}

use protocol::IrohAutomergeProtocol;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args: Vec<String> = std::env::args().collect();
    let usage = "usage: automerge_peer listen | connect <id> <ip:port> | vectors";
    match args.get(1).map(String::as_str) {
        Some("listen") => listen().await,
        Some("connect") => {
            let id: EndpointId = args.get(2).context(usage)?.parse()?;
            let addr: SocketAddr = args.get(3).context(usage)?.parse()?;
            connect(id, addr).await
        }
        Some("vectors") => vectors(),
        _ => anyhow::bail!(usage),
    }
}

/// rust_doc is the document the Rust side starts with. "shared" is also set by
/// the Go side, so the two start in conflict and must agree on a winner.
fn rust_doc() -> anyhow::Result<Automerge> {
    let mut doc = Automerge::new();
    let mut t = doc.transaction();
    for i in 0..3 {
        t.put(
            automerge::ROOT,
            format!("rust-{i}"),
            format!("from rust {i}"),
        )?;
    }
    t.put(automerge::ROOT, "shared", "rust")?;
    t.commit();
    Ok(doc)
}

async fn bind() -> anyhow::Result<Endpoint> {
    Ok(Endpoint::builder(presets::N0DisableRelay)
        .bind_addr("127.0.0.1:0".parse::<SocketAddr>().unwrap())?
        .bind()
        .await?)
}

async fn listen() -> anyhow::Result<()> {
    let (tx, mut synced) = mpsc::channel(1);
    let proto = IrohAutomergeProtocol::new(rust_doc()?, tx);
    let ep = bind().await?;
    let addr = ep.addr();
    for ta in addr.addrs.iter() {
        if let TransportAddr::Ip(sa) = ta {
            println!("ADDR {} {}", addr.id, sa);
        }
    }
    println!("READY");
    let router = Router::builder(ep)
        .accept(IrohAutomergeProtocol::ALPN, proto)
        .spawn();

    // accept sends the document once respond_sync has finished, which is after
    // the dialer has closed the connection.
    let doc = tokio::time::timeout(Duration::from_secs(20), synced.recv())
        .await
        .context("timed out waiting for a sync")?
        .context("protocol handler dropped")?;
    print_doc(&doc)?;
    router.shutdown().await?;
    Ok(())
}

async fn connect(id: EndpointId, sa: SocketAddr) -> anyhow::Result<()> {
    let (tx, _synced) = mpsc::channel(1);
    let proto = IrohAutomergeProtocol::new(rust_doc()?, tx);
    let ep = bind().await?;
    let addr = EndpointAddr::from_parts(id, [TransportAddr::Ip(sa)]);
    let conn = ep.connect(addr, IrohAutomergeProtocol::ALPN).await?;
    Arc::clone(&proto).initiate_sync(conn).await?;
    print_doc(&proto.fork_doc().await)?;
    ep.close().await;
    Ok(())
}

fn print_doc(doc: &Automerge) -> anyhow::Result<()> {
    println!("State");
    // keys are returned in sorted order.
    for key in doc.keys(automerge::ROOT) {
        let (value, _) = doc.get(automerge::ROOT, &key)?.context("key vanished")?;
        let s = value
            .to_str()
            .with_context(|| format!("{key}: not a string"))?;
        println!("{key} => {s:?}");
    }
    Ok(())
}

/// vectors prints bytes the Go tests pin: the ALPN, and the first sync message
/// an empty document sends, which is the first thing a peer ever receives.
fn vectors() -> anyhow::Result<()> {
    println!("upstream iroh-examples {}", env!("IROH_AUTOMERGE_REV"));
    println!("alpn {}", hex(IrohAutomergeProtocol::ALPN));
    let doc = Automerge::new();
    let msg = doc
        .generate_sync_message(&mut sync::State::new())
        .context("empty doc sends no message")?;
    println!("empty-doc first sync message {}", hex(&msg.encode()));
    Ok(())
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}
