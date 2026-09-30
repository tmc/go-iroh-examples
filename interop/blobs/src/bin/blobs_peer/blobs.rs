//! The iroh-blobs provider and getter. Both are the crate's own code: the
//! provider is BlobsProtocol over a MemStore, as in iroh-blobs'
//! examples/transfer.rs, and the getter is get::request::get_blob, which
//! verifies every chunk against the hash it was given.

use std::net::SocketAddr;
use std::path::Path;

use anyhow::Context as _;
use iroh::{EndpointAddr, EndpointId, TransportAddr, protocol::Router};
use iroh_blobs::{
    ALPN, BlobFormat, BlobsProtocol, Hash, format::collection::Collection, store::mem::MemStore,
    ticket::BlobTicket,
};

/// provide serves each file as a raw blob, and all of them together as a
/// collection named by file base name, until it is killed. It prints
///
///   ADDR <id> <ip:port>
///   BLOB <name> <hash>              one per file
///   COLLECTION <hash>
///   BLOB_TICKET <ticket>            the first file, as a raw blob ticket
///   COLLECTION_TICKET <ticket>
///   READY
pub async fn provide(files: &[&str]) -> anyhow::Result<()> {
    let store = MemStore::new();
    let mut collection = Collection::default();
    let mut blob_lines = Vec::new();
    for file in files {
        let data = std::fs::read(file).with_context(|| format!("read {file}"))?;
        let name = Path::new(file)
            .file_name()
            .context("file has no name")?
            .to_string_lossy()
            .into_owned();
        let tag = store.add_bytes(data).await?;
        blob_lines.push(format!("BLOB {name} {}", tag.hash));
        collection.push(name, tag.hash);
    }
    let first = collection.iter().next().map(|(_, h)| *h);
    let root = collection.store(&store).await?;

    let bind = "127.0.0.1:0".parse()?;
    let ep = super::bind(bind, vec![ALPN.to_vec()]).await?;
    let addr = super::loopback_addr(&ep, bind)?;
    let router = Router::builder(ep)
        .accept(ALPN, BlobsProtocol::new(&store, None))
        .spawn();

    super::print_addr(&addr);
    for line in blob_lines {
        println!("{line}");
    }
    println!("COLLECTION {}", root.hash());
    if let Some(first) = first {
        println!(
            "BLOB_TICKET {}",
            BlobTicket::new(addr.clone(), first, BlobFormat::Raw)
        );
    }
    println!(
        "COLLECTION_TICKET {}",
        BlobTicket::new(addr, root.hash(), BlobFormat::HashSeq)
    );
    println!("READY");

    tokio::signal::ctrl_c().await?;
    router.shutdown().await?;
    Ok(())
}

/// get fetches hash from the provider id at addr and writes it to out. The
/// bytes are verified as they arrive, so a provider that sends anything but
/// the content hash names fails here.
pub async fn get(id: &str, addr: &str, hash: &str, out: &str) -> anyhow::Result<()> {
    let id: EndpointId = id.parse()?;
    let sa: SocketAddr = addr.parse()?;
    let hash: Hash = hash.parse()?;

    let ep = super::bind(super::dial_bind(sa), vec![]).await?;
    let conn = ep
        .connect(EndpointAddr::from_parts(id, [TransportAddr::Ip(sa)]), ALPN)
        .await?;
    let data = iroh_blobs::get::request::get_blob(conn.clone(), hash)
        .bytes()
        .await
        .context("get blob")?;
    std::fs::write(out, &data)?;
    println!("GOT {} bytes", data.len());
    conn.close(0u32.into(), b"done");
    ep.close().await;
    Ok(())
}
