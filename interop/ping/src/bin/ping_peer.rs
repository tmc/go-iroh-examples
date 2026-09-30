//! A Rust iroh peer built from n0's iroh-ping crate, so go-iroh-ping is tested
//! against it rather than described as compatible with it.
//!
//!   ping_peer vectors                    print "ALPN <hex>" of iroh_ping::ALPN
//!   ping_peer listen                     bind, print "ADDR <id> <ip:port>", serve ping
//!   ping_peer connect <id> <ip:port>     ping that endpoint, print "PONG <rtt>"
//!
//! Both halves are the crate's own: the listener registers iroh_ping::Ping with
//! a Router, and the dialer calls Ping::ping, which sends PING and asserts that
//! PONG comes back. Nothing about the wire is written out here.
//!
//! Relays are disabled and only direct IP addresses are used, so a test needs
//! no network.

use std::net::SocketAddr;

use anyhow::Context as _;
use iroh::{Endpoint, EndpointAddr, EndpointId, TransportAddr, endpoint::presets, protocol::Router};
use iroh_ping::{ALPN, Ping};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args: Vec<String> = std::env::args().collect();
    match args.get(1).map(String::as_str) {
        Some("vectors") => {
            println!("ALPN {}", hex::encode(ALPN));
            Ok(())
        }
        Some("listen") => listen().await,
        Some("connect") => {
            let id: EndpointId = args.get(2).context("usage: connect <id> <ip:port>")?.parse()?;
            let addr: SocketAddr = args.get(3).context("usage: connect <id> <ip:port>")?.parse()?;
            connect(id, addr).await
        }
        _ => anyhow::bail!("usage: ping_peer vectors | ping_peer listen | ping_peer connect <id> <ip:port>"),
    }
}

async fn bind() -> anyhow::Result<Endpoint> {
    Ok(Endpoint::builder(presets::N0DisableRelay)
        .bind_addr("127.0.0.1:0".parse::<SocketAddr>().unwrap())?
        .bind()
        .await?)
}

/// The listener half: iroh_ping::Ping behind a Router, served until the process
/// is killed. Ping's handler prints "accepted connection from <id>" per dialer.
async fn listen() -> anyhow::Result<()> {
    let ep = bind().await?;
    let router = Router::builder(ep.clone()).accept(ALPN, Ping::new()).spawn();
    let addr = ep.addr();
    for ta in addr.addrs.iter() {
        if let TransportAddr::Ip(sa) = ta {
            println!("ADDR {} {}", addr.id, sa);
        }
    }
    println!("READY");
    tokio::signal::ctrl_c().await?;
    router.shutdown().await?;
    Ok(())
}

/// The dialer half: Ping::ping, which fails (by panic) unless PONG comes back.
async fn connect(id: EndpointId, sa: SocketAddr) -> anyhow::Result<()> {
    let ep = bind().await?;
    let rtt = Ping::new()
        .ping(&ep, EndpointAddr::from_parts(id, [TransportAddr::Ip(sa)]))
        .await?;
    println!("PONG {rtt:?}");
    ep.close().await;
    Ok(())
}
