//! A Rust iroh peer that speaks the framed-messages chess protocol, so the Go
//! examples can be tested against the implementation they claim compatibility
//! with rather than against themselves.
//!
//!   peer listen                  bind, print "ADDR <id> <ip:port>", play black
//!   peer connect <id> <ip:port>  dial that address and play white
//!
//! Both modes disable relays and use only direct IP addresses, so a test needs
//! no network and no infrastructure.

use std::net::SocketAddr;

use anyhow::Context as _;
use framed_messages::{ALPN, ChessProtocol, Move, framed::FramedBiStream};
use iroh::{
    Endpoint, EndpointAddr, EndpointId,
    TransportAddr,
    endpoint::presets,
    protocol::Router,
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args: Vec<String> = std::env::args().collect();
    match args.get(1).map(String::as_str) {
        Some("listen") => listen().await,
        Some("connect") => {
            let id: EndpointId = args.get(2).context("usage: peer connect <id> <ip:port>")?.parse()?;
            let addr: SocketAddr = args.get(3).context("usage: peer connect <id> <ip:port>")?.parse()?;
            connect(id, addr).await
        }
        _ => anyhow::bail!("usage: peer listen | peer connect <id> <ip:port>"),
    }
}

async fn listen() -> anyhow::Result<()> {
    // Bind loopback explicitly so the announced address is dialable from the
    // same host with no network, matching how the Go examples bind.
    let ep = Endpoint::builder(presets::N0DisableRelay)
        .bind_addr("127.0.0.1:0".parse::<SocketAddr>().unwrap())?
        .bind()
        .await?;
    let addr = ep.addr();
    for ta in addr.addrs.iter() {
        if let TransportAddr::Ip(sa) = ta {
            println!("ADDR {} {}", addr.id, sa);
        }
    }
    println!("READY");
    let router = Router::builder(ep).accept(ALPN, ChessProtocol).spawn();
    // One exchange is enough for a test; ChessProtocol waits for the dialer to
    // close, so returning here after a pause ends the process cleanly.
    tokio::time::sleep(std::time::Duration::from_secs(20)).await;
    router.shutdown().await?;
    Ok(())
}

async fn connect(id: EndpointId, sa: SocketAddr) -> anyhow::Result<()> {
    let ep = Endpoint::builder(presets::N0DisableRelay)
        .bind_addr("127.0.0.1:0".parse::<SocketAddr>().unwrap())?
        .bind()
        .await?;
    let addr = EndpointAddr::from_parts(id, [TransportAddr::Ip(sa)]);
    let conn = ep.connect(addr, ALPN).await?;
    let bi = conn.open_bi().await?;
    let mut stream = FramedBiStream::new(bi);

    // White's moves, matching the Go example's client half.
    Move { from: (4, 2), to: (4, 4) }.send(&mut stream).await?;
    let mv = Move::recv(&mut stream).await?;
    println!("got move: {:?}", mv);
    Move { from: (3, 2), to: (3, 3) }.send(&mut stream).await?;
    let mv = Move::recv(&mut stream).await?;
    println!("got move: {:?}", mv);

    conn.close(0u32.into(), b"bye!");
    ep.close().await;
    Ok(())
}
