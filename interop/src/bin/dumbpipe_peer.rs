//! A Rust iroh peer that speaks n0's dumbpipe protocol, so go-iroh-dumbpipe is
//! tested against it rather than described as compatible with it.
//!
//!   dumbpipe_peer listen                       bind, print "ADDR <id> <ip:port>", echo one stream
//!   dumbpipe_peer connect <id> <ip:port> <s>   dial, send s, print what comes back
//!
//! ALPN and HANDSHAKE come from the dumbpipe crate rather than being written
//! out here, so the day upstream changes either one this stops compiling or
//! stops agreeing instead of quietly testing a copy of the old protocol.
//!
//! Relays are disabled and only direct IP addresses are used, so a test needs
//! no network.

use std::net::SocketAddr;

use anyhow::Context as _;
use dumbpipe::{ALPN, HANDSHAKE};
use iroh::{Endpoint, EndpointAddr, EndpointId, TransportAddr, endpoint::presets};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args: Vec<String> = std::env::args().collect();
    match args.get(1).map(String::as_str) {
        Some("listen") => listen().await,
        Some("connect") => {
            let id: EndpointId = args.get(2).context("usage: connect <id> <ip:port> <text>")?.parse()?;
            let addr: SocketAddr = args.get(3).context("usage: connect <id> <ip:port> <text>")?.parse()?;
            let text = args.get(4).context("usage: connect <id> <ip:port> <text>")?.clone();
            connect(id, addr, text).await
        }
        _ => anyhow::bail!("usage: dumbpipe_peer listen | dumbpipe_peer connect <id> <ip:port> <text>"),
    }
}

async fn bind() -> anyhow::Result<Endpoint> {
    Ok(Endpoint::builder(presets::N0DisableRelay)
        .alpns(vec![ALPN.to_vec()])
        .bind_addr("127.0.0.1:0".parse::<SocketAddr>().unwrap())?
        .bind()
        .await?)
}

/// The listener half: verify the dialer's handshake, then echo the payload back
/// with a prefix so the Go side can tell a real round trip from an empty read.
async fn listen() -> anyhow::Result<()> {
    let ep = bind().await?;
    let addr = ep.addr();
    for ta in addr.addrs.iter() {
        if let TransportAddr::Ip(sa) = ta {
            println!("ADDR {} {}", addr.id, sa);
        }
    }
    println!("READY");

    let conn = ep.accept().await.context("accept")?.await?;
    let (mut send, mut recv) = conn.accept_bi().await?;

    let mut hs = [0u8; HANDSHAKE.len()];
    recv.read_exact(&mut hs).await?;
    anyhow::ensure!(hs == HANDSHAKE, "invalid handshake from the Go peer");

    let body = recv.read_to_end(64 * 1024).await?;
    send.write_all(b"rust echo: ").await?;
    send.write_all(&body).await?;
    send.finish()?;
    conn.closed().await;
    Ok(())
}

/// The dialer half: write the handshake before any payload, as dumbpipe does.
async fn connect(id: EndpointId, sa: SocketAddr, text: String) -> anyhow::Result<()> {
    let ep = bind().await?;
    let conn = ep
        .connect(EndpointAddr::from_parts(id, [TransportAddr::Ip(sa)]), ALPN)
        .await?;
    let (mut send, mut recv) = conn.open_bi().await?;
    send.write_all(&HANDSHAKE).await?;
    send.write_all(text.as_bytes()).await?;
    send.finish()?;

    let body = recv.read_to_end(64 * 1024).await?;
    print!("{}", String::from_utf8_lossy(&body));
    conn.close(0u32.into(), b"bye!");
    ep.close().await;
    Ok(())
}
