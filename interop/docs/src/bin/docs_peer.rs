//! A Rust iroh-docs peer, so go-iroh-docs-sync and go-iroh-docs-live-sync can
//! be tested against the implementation their docs package ports.
//!
//!   docs_peer <namespace-secret-hex> <author-secret-hex> [key=value ...]
//!
//! The peer binds 127.0.0.1 with relays disabled and serves the three
//! protocols an iroh-docs node serves: /iroh-sync/1, iroh-gossip and
//! iroh-blobs. It imports the namespace with write capability, writes the
//! given entries as the given author, opens the document for sync, prints
//! "ADDR <id> <ip:port>" and "READY", and then reads commands from stdin:
//!
//!   sync <id> <ip:port>   start syncing with that peer (Doc::start_sync)
//!   put <key> <value>     write one more entry
//!   dump                  print every entry, then "END"
//!
//! It exits at end of input. Document events are printed as they happen, as
//! "EVENT <kind> ..." lines, so a test can wait for a sync to finish or for
//! content to arrive. Everything that syncs, gossips or downloads here is
//! upstream code: the node is iroh_docs::protocol::Docs over an
//! iroh_blobs::store::mem::MemStore and an iroh_gossip::net::Gossip, set up
//! the way the iroh-docs setup example does it.

use std::net::SocketAddr;

use anyhow::{Context as _, bail};
use futures_lite::StreamExt as _;
use iroh::{
    Endpoint, EndpointAddr, EndpointId, TransportAddr, endpoint::presets, protocol::Router,
};
use iroh_blobs::{ALPN as BLOBS_ALPN, BlobsProtocol, api::blobs::BlobStatus, store::mem::MemStore};
use iroh_docs::{
    ALPN as DOCS_ALPN, Author, Capability, NamespaceSecret, SignedEntry,
    engine::{DefaultAuthorStorage, Engine, LiveEvent},
    protocol::Docs,
    store::Query,
};
use iroh_gossip::{ALPN as GOSSIP_ALPN, net::Gossip};
use tokio::io::{AsyncBufReadExt as _, BufReader};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // RUST_LOG=iroh_docs=debug shows why a sync was refused, on stderr.
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_writer(std::io::stderr)
        .init();
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 3 {
        bail!("usage: docs_peer <namespace-secret-hex> <author-secret-hex> [key=value ...]");
    }
    let namespace = NamespaceSecret::from_bytes(&parse32(&args[1]).context("namespace secret")?);
    let author = Author::from_bytes(&parse32(&args[2]).context("author secret")?);

    let ep = Endpoint::builder(presets::N0DisableRelay)
        .bind_addr("127.0.0.1:0".parse::<SocketAddr>().unwrap())?
        .bind()
        .await?;
    let blobs = MemStore::new();
    let gossip = Gossip::builder().spawn(ep.clone());

    // Docs::memory().spawn would do, but it keeps the engine to itself, and
    // the engine's SyncHandle is the only way to read entries with their
    // signatures: the Doc API returns them unsigned.
    let engine = Engine::spawn(
        ep.clone(),
        gossip.clone(),
        iroh_docs::store::Store::memory(),
        (*blobs).clone(),
        blobs.downloader(&ep),
        DefaultAuthorStorage::Mem,
        None,
    )
    .await?;
    let replicas = engine.sync.clone();
    let docs = Docs::new(engine);
    let router = Router::builder(ep.clone())
        .accept(BLOBS_ALPN, BlobsProtocol::new(&blobs, None))
        .accept(GOSSIP_ALPN, gossip)
        .accept(DOCS_ALPN, docs.clone())
        .spawn();

    docs.author_import(author.clone()).await?;
    let doc = docs.import_namespace(Capability::Write(namespace)).await?;
    for kv in &args[3..] {
        let (k, v) = kv.split_once('=').context("entries are key=value")?;
        doc.set_bytes(author.id(), k.as_bytes().to_vec(), v.as_bytes().to_vec())
            .await?;
    }

    let mut events = doc.subscribe().await?;
    tokio::spawn(async move {
        while let Some(ev) = events.next().await {
            match ev {
                Ok(ev) => print_event(ev),
                Err(err) => println!("EVENT Error {err:#}"),
            }
        }
    });

    // Starting sync with no peers is what puts the namespace in the set the
    // engine accepts sync requests for.
    doc.start_sync(vec![]).await?;

    let addr = ep.addr();
    for ta in addr.addrs.iter() {
        if let TransportAddr::Ip(sa) = ta {
            println!("ADDR {} {}", addr.id, sa);
        }
    }
    println!("READY");

    let mut lines = BufReader::new(tokio::io::stdin()).lines();
    while let Some(line) = lines.next_line().await? {
        let fields: Vec<&str> = line.split_whitespace().collect();
        match fields.as_slice() {
            ["sync", id, sa] => {
                let id: EndpointId = id.parse()?;
                let sa: SocketAddr = sa.parse()?;
                doc.start_sync(vec![EndpointAddr::from_parts(id, [TransportAddr::Ip(sa)])])
                    .await?;
            }
            ["put", k, v] => {
                doc.set_bytes(author.id(), k.as_bytes().to_vec(), v.as_bytes().to_vec())
                    .await?;
            }
            ["dump"] => {
                let (tx, mut rx) = irpc::channel::mpsc::channel(64);
                replicas.get_many(doc.id(), Query::all().build(), tx).await?;
                while let Some(entry) = rx.recv().await? {
                    print_entry(&blobs, &entry?).await?;
                }
                println!("END");
            }
            [] => {}
            _ => bail!("unknown command {line:?}"),
        }
    }
    router.shutdown().await?;
    Ok(())
}

/// print_entry prints one entry and, if the content is stored locally, the
/// content itself:
///
///   ENTRY <author> <key> <hash> <len> <timestamp> <signature> <content|->
///
/// The signature is the postcard encoding of EntrySignature, which is the
/// author signature followed by the namespace signature.
async fn print_entry(blobs: &MemStore, entry: &SignedEntry) -> anyhow::Result<()> {
    let sig = postcard::to_stdvec(entry.signature())?;
    let content = match blobs.blobs().status(entry.content_hash()).await? {
        BlobStatus::Complete { .. } => {
            hex::encode(blobs.blobs().get_bytes(entry.content_hash()).await?)
        }
        _ => "-".to_string(),
    };
    println!(
        "ENTRY {} {} {} {} {} {} {}",
        hex::encode(entry.author().as_bytes()),
        hex::encode(entry.key()),
        entry.content_hash().to_hex(),
        entry.content_len(),
        entry.timestamp(),
        hex::encode(sig),
        content,
    );
    Ok(())
}

fn print_event(ev: LiveEvent) {
    match ev {
        LiveEvent::InsertLocal { entry } => {
            println!("EVENT InsertLocal {}", hex::encode(entry.key()))
        }
        LiveEvent::InsertRemote { from, entry, content_status } => println!(
            "EVENT InsertRemote {} {} {:?}",
            from,
            hex::encode(entry.key()),
            content_status
        ),
        LiveEvent::ContentReady { hash } => println!("EVENT ContentReady {}", hash.to_hex()),
        LiveEvent::PendingContentReady => println!("EVENT PendingContentReady"),
        LiveEvent::NeighborUp(peer) => println!("EVENT NeighborUp {peer}"),
        LiveEvent::NeighborDown(peer) => println!("EVENT NeighborDown {peer}"),
        LiveEvent::SyncFinished(ev) => match ev.result {
            Ok(d) => println!(
                "EVENT SyncFinished {} ok sent={} received={}",
                ev.peer, d.entries_sent, d.entries_received
            ),
            Err(err) => println!("EVENT SyncFinished {} err {err}", ev.peer),
        },
    }
}

fn parse32(s: &str) -> anyhow::Result<[u8; 32]> {
    let b = hex::decode(s)?;
    b.try_into()
        .map_err(|b: Vec<u8>| anyhow::anyhow!("want 32 bytes, got {}", b.len()))
}
