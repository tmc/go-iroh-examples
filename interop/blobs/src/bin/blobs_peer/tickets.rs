//! Endpoint tickets, parsed and printed by iroh-tickets, dialed on the
//! go-iroh-tickets example's echo protocol.

use std::net::SocketAddr;

use anyhow::Context as _;
use iroh_tickets::endpoint::EndpointTicket;

/// ALPN is go-iroh-tickets' own protocol, not an upstream one: the example
/// echoes one stream, and that is all a ticket needs to be shown to work.
const ALPN: &[u8] = b"go-iroh-examples/tickets/1";

/// listen binds addr, prints "TICKET <ticket>" and "READY", and echoes one
/// stream. The ticket lists only the loopback address in addr's family, so an
/// IPv4 bind gives an IPv4-only ticket.
pub async fn listen(addr: &str) -> anyhow::Result<()> {
    let bind: SocketAddr = addr.parse()?;
    let ep = super::bind(bind, vec![ALPN.to_vec()]).await?;
    let ticket = EndpointTicket::new(super::loopback_addr(&ep, bind)?);
    println!("TICKET {ticket}");
    println!("READY");

    let conn = ep.accept().await.context("accept")?.await?;
    let (mut send, mut recv) = conn.accept_bi().await?;
    let body = recv.read_to_end(64 * 1024).await?;
    send.write_all(&body).await?;
    send.finish()?;
    conn.closed().await;
    Ok(())
}

/// dial parses ticket, dials the address inside it, and prints the echo of
/// text. A ticket that does not parse fails here with iroh-tickets' error.
pub async fn dial(ticket: &str, text: &str) -> anyhow::Result<()> {
    let ticket: EndpointTicket = ticket.parse().context("parse ticket")?;
    let target = ticket
        .endpoint_addr()
        .ip_addrs()
        .next()
        .copied()
        .context("ticket has no IP address")?;
    let ep = super::bind(super::dial_bind(target), vec![]).await?;
    let conn = ep.connect(ticket.endpoint_addr().clone(), ALPN).await?;
    let (mut send, mut recv) = conn.open_bi().await?;
    send.write_all(text.as_bytes()).await?;
    send.finish()?;
    let body = recv.read_to_end(64 * 1024).await?;
    println!("ECHO {}", String::from_utf8_lossy(&body));
    conn.close(0u32.into(), b"bye");
    ep.close().await;
    Ok(())
}
