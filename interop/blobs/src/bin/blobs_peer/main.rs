//! A Rust iroh peer for the Go examples whose claims rest on the iroh-blobs
//! crate, iroh's tickets, and postcard: go-iroh-blobs-transfer,
//! go-iroh-blobs-gateway, go-iroh-tickets and go-iroh-key-exchange.
//!
//!   blobs_peer provide <file>...                    serve the files over iroh-blobs
//!   blobs_peer get <id> <ip:port> <hash> <out>      fetch one blob into out
//!   blobs_peer ticket-listen <ip:port>              print a ticket, echo one stream
//!   blobs_peer ticket-dial <ticket> <text>          dial a ticket, print the echo
//!   blobs_peer kx-vectors                           print postcard encodings
//!   blobs_peer kx-listen <policy> <ip:port>         answer one key-exchange request
//!   blobs_peer kx-connect <policy> <id> <ip:port> <nonce>
//!
//! Listeners print "ADDR <id> <ip:port>" (or "TICKET <ticket>") and then
//! "READY". Relays are disabled and only direct loopback addresses are used,
//! so a test needs no network.

mod blobs;
mod kx;
mod tickets;

use std::net::SocketAddr;

use anyhow::Context as _;
use iroh::{Endpoint, EndpointAddr, TransportAddr, endpoint::presets};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let args: Vec<&str> = args.iter().map(String::as_str).collect();
    match args.as_slice() {
        ["provide", files @ ..] => blobs::provide(files).await,
        ["get", id, addr, hash, out] => blobs::get(id, addr, hash, out).await,
        ["ticket-listen", addr] => tickets::listen(addr).await,
        ["ticket-dial", ticket, text] => tickets::dial(ticket, text).await,
        ["kx-vectors"] => kx::vectors(),
        ["kx-listen", policy, addr] => kx::listen(policy, addr).await,
        ["kx-connect", policy, id, addr, nonce] => kx::connect(policy, id, addr, nonce).await,
        _ => anyhow::bail!("usage: see the comment at the top of blobs_peer/main.rs"),
    }
}

/// bind binds an endpoint on addr with relays disabled, accepting alpns.
async fn bind(addr: SocketAddr, alpns: Vec<Vec<u8>>) -> anyhow::Result<Endpoint> {
    Ok(Endpoint::builder(presets::N0DisableRelay)
        .alpns(alpns)
        .bind_addr(addr)?
        .bind()
        .await?)
}

/// dial_bind is the address a dialer binds to reach target: loopback, in
/// target's address family, since a socket on one has no route to the other.
fn dial_bind(target: SocketAddr) -> SocketAddr {
    if target.is_ipv4() {
        "127.0.0.1:0".parse().unwrap()
    } else {
        "[::1]:0".parse().unwrap()
    }
}

/// loopback_addr is ep's address restricted to its loopback socket in the
/// family of bind. The builder also binds the unspecified addresses, so
/// ep.addr() lists every interface; the tests need only the one they can
/// reach.
fn loopback_addr(ep: &Endpoint, bind: SocketAddr) -> anyhow::Result<EndpointAddr> {
    let addr = ep.addr();
    let sa = addr
        .addrs
        .iter()
        .find_map(|ta| match ta {
            TransportAddr::Ip(sa) if sa.ip().is_loopback() && sa.is_ipv4() == bind.is_ipv4() => {
                Some(*sa)
            }
            _ => None,
        })
        .context("endpoint has no loopback address")?;
    Ok(EndpointAddr::from_parts(addr.id, [TransportAddr::Ip(sa)]))
}

/// print_addr prints the "ADDR <id> <ip:port>" line the Go tests read.
fn print_addr(addr: &EndpointAddr) {
    for ta in addr.addrs.iter() {
        if let TransportAddr::Ip(sa) = ta {
            println!("ADDR {} {}", addr.id, sa);
        }
    }
}
