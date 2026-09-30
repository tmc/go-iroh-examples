//! A Rust iroh peer that finds other endpoints with n0's mDNS address lookup,
//! iroh-mdns-address-lookup, so the Go mDNS example can be tested against the
//! implementation whose service name it claims to share.
//!
//!   mdns_peer listen           print "ID <id>", wait to be found and dialed, echo once
//!   mdns_peer dial <id>        dial <id> by ID alone and echo "mdns hello"
//!   mdns_peer announce         announce fixed endpoint data, print "ID <id>"
//!   mdns_peer resolve <id>     resolve <id> and print what came back
//!
//! Every endpoint disables relays and has mDNS as its only address lookup, so
//! an ID can become an address only through mDNS. The service name is the
//! crate's default, which is the point of the test. announce and resolve use a
//! bare MdnsAddressLookup with no endpoint, so that the relay URL and user data
//! travel in the TXT record and the keys that carry them are exercised too.

use std::{net::SocketAddr, time::Duration};

use anyhow::Context as _;
use futures_util::StreamExt as _;
use iroh::{
    Endpoint, EndpointAddr, EndpointId, RelayMode, SecretKey, TransportAddr,
    address_lookup::{AddressLookup as _, EndpointData, UserData},
    endpoint::{Connection, presets},
    protocol::{AcceptError, ProtocolHandler, Router},
};
use iroh_mdns_address_lookup::MdnsAddressLookup;

/// The Go example's ALPN and its protocol: the dialer opens a stream, writes a
/// message, finishes, and reads the echo to the end.
const ALPN: &[u8] = b"go-iroh-examples/mdns-discovery/1";

/// What announce publishes, and what the Go test expects to resolve. The two
/// addresses have different ports, which swarm-discovery announces as two SRV
/// records for one instance, the way it announces an endpoint bound to both
/// 0.0.0.0 and [::].
const ANNOUNCE_ADDRS: [&str; 2] = ["127.0.0.1:4242", "[::1]:4243"];
const ANNOUNCE_RELAY: &str = "https://relay.example.com./";
const ANNOUNCE_USER_DATA: &str = "rust-announce";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // RUST_LOG=iroh_mdns_address_lookup=trace,swarm_discovery=trace shows
    // every packet the lookup sends and hears.
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_writer(std::io::stderr)
        .init();
    let args: Vec<String> = std::env::args().collect();
    let id = |i: usize| -> anyhow::Result<EndpointId> {
        Ok(args.get(i).context("missing endpoint id")?.parse()?)
    };
    match args.get(1).map(String::as_str) {
        Some("listen") => listen().await,
        Some("dial") => dial(id(2)?).await,
        Some("announce") => announce().await,
        Some("resolve") => resolve(id(2)?).await,
        _ => anyhow::bail!("usage: mdns_peer listen | dial <id> | announce | resolve <id>"),
    }
}

/// bind binds an endpoint to IPv4 loopback alone, whose only address lookup
/// is mDNS with the default service name. Clearing the default IP transports
/// leaves one socket and so one port: iroh otherwise also binds [::]:0, whose
/// addresses swarm-discovery announces under a second SRV record.
async fn bind(advertise: bool) -> anyhow::Result<Endpoint> {
    let ep = Endpoint::builder(presets::Minimal)
        .relay_mode(RelayMode::Disabled)
        .clear_ip_transports()
        .bind_addr("127.0.0.1:0".parse::<SocketAddr>().unwrap())?
        .address_lookup(MdnsAddressLookup::builder().advertise(advertise))
        .bind()
        .await?;
    Ok(ep)
}

async fn listen() -> anyhow::Result<()> {
    let ep = bind(true).await?;
    ep.set_user_data_for_address_lookup(Some("rust-listen".parse()?));
    println!("ID {}", ep.id());
    println!("READY");
    let router = Router::builder(ep).accept(ALPN, Echo).spawn();
    tokio::time::sleep(Duration::from_secs(30)).await;
    router.shutdown().await?;
    Ok(())
}

async fn dial(id: EndpointId) -> anyhow::Result<()> {
    let ep = bind(false).await?;
    // The address holds the ID and nothing else.
    let conn = ep.connect(EndpointAddr::from(id), ALPN).await?;
    let (mut send, mut recv) = conn.open_bi().await?;
    send.write_all(b"mdns hello").await?;
    send.finish()?;
    let reply = recv.read_to_end(1024).await?;
    println!("reply: {}", String::from_utf8_lossy(&reply));
    conn.close(0u32.into(), b"bye");
    ep.close().await;
    Ok(())
}

async fn announce() -> anyhow::Result<()> {
    let id = SecretKey::generate().public();
    let mdns = MdnsAddressLookup::builder().build(id)?;
    let mut addrs = Vec::new();
    for a in ANNOUNCE_ADDRS {
        addrs.push(TransportAddr::Ip(a.parse()?));
    }
    addrs.push(TransportAddr::Relay(ANNOUNCE_RELAY.parse()?));
    let data =
        EndpointData::from_iter(addrs).with_user_data(ANNOUNCE_USER_DATA.parse::<UserData>()?);
    mdns.publish(&data);
    println!("ID {id}");
    println!("READY");
    tokio::time::sleep(Duration::from_secs(30)).await;
    Ok(())
}

async fn resolve(id: EndpointId) -> anyhow::Result<()> {
    let mdns = MdnsAddressLookup::builder()
        .advertise(false)
        .build(SecretKey::generate().public())?;
    let mut items = mdns.resolve(id).context("mdns does not resolve")?;
    let item = tokio::time::timeout(Duration::from_secs(15), items.next())
        .await
        .context("no mDNS answer")?
        .context("resolve stream ended")??;
    println!("provenance: {}", item.provenance());
    for sa in item.ip_addrs() {
        println!("addr: {sa}");
    }
    for url in item.relay_urls() {
        println!("relay: {url}");
    }
    if let Some(u) = item.user_data() {
        println!("user-data: {u}");
    }
    Ok(())
}

#[derive(Debug, Clone)]
struct Echo;

impl ProtocolHandler for Echo {
    async fn accept(&self, conn: Connection) -> Result<(), AcceptError> {
        let (mut send, mut recv) = conn.accept_bi().await?;
        let b = recv
            .read_to_end(1024)
            .await
            .map_err(AcceptError::from_err)?;
        send.write_all(&b).await.map_err(AcceptError::from_err)?;
        send.finish()?;
        conn.closed().await;
        Ok(())
    }
}
