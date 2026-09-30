//! A Rust iroh-gossip peer, so the Go gossip examples can be tested against
//! the implementation they claim compatibility with rather than against
//! themselves.
//!
//!   gossip_peer topic <topic-hex> [<id> <ip:port>]  raw iroh-gossip topic
//!   gossip_peer kv <topic-hex> [<id> <ip:port>]     iroh-smol-kv store on the topic
//!   gossip_peer kv-vector                           print one smol-kv message as hex
//!
//! The live modes bind 127.0.0.1 with relays disabled, print
//! "ADDR <id> <ip:port>" and "READY", and subscribe to the topic, joining the
//! bootstrap peer when one is given. They then report on stdout and take
//! commands on stdin, one per line, until stdin closes:
//!
//!   both:  stdin "join <id> <ip:port>" joins that peer later, for a caller
//!          that must learn this peer's id before it can be dialed.
//!   topic: stdout "UP <id>", "DOWN <id>", "RECV <from-id> <content>";
//!          stdin "broadcast <text>".
//!   kv:    stdout "SCOPE <id>", "UP <id>", "DOWN <id>",
//!          "ENTRY <scope> <key> <value>" for every entry the store holds;
//!          stdin "put <key> <value>".
//!
//! Nothing here speaks the wire protocol itself: the topic is iroh-gossip's
//! and the store is iroh-smol-kv's, from crates.io.

use std::net::SocketAddr;

use anyhow::Context as _;
use bytes::Bytes;
use iroh::{
    Endpoint, EndpointAddr, EndpointId, SecretKey, TransportAddr,
    address_lookup::memory::MemoryLookup, endpoint::presets, protocol::Router,
};
use iroh_gossip::{
    api::{Event, GossipReceiver},
    net::Gossip,
    proto::TopicId,
};
use iroh_smol_kv::{Client, Config, proto::{GossipMessage, SignedValue}};
use n0_future::StreamExt;
use serde::Serialize;
use tokio::io::{AsyncBufReadExt, BufReader};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args: Vec<String> = std::env::args().collect();
    const USAGE: &str = "usage: gossip_peer topic|kv <topic-hex> [<id> <ip:port>] | kv-vector";
    let mode = args.get(1).map(String::as_str);
    if mode == Some("kv-vector") {
        return kv_vector();
    }
    let topic: [u8; 32] = hex::decode(args.get(2).context(USAGE)?)?
        .try_into()
        .map_err(|_| anyhow::anyhow!("topic must be 32 bytes"))?;
    let topic = TopicId::from_bytes(topic);
    let bootstrap = match (args.get(3), args.get(4)) {
        (Some(id), Some(addr)) => Some(parse_addr(id, addr)?),
        (None, None) => None,
        _ => anyhow::bail!(USAGE),
    };
    let peer = Peer::bind(bootstrap).await?;
    match mode {
        Some("topic") => peer.topic(topic).await,
        Some("kv") => peer.kv(topic).await,
        _ => anyhow::bail!(USAGE),
    }
}

fn parse_addr(id: &str, addr: &str) -> anyhow::Result<EndpointAddr> {
    let id: EndpointId = id.parse()?;
    let addr: SocketAddr = addr.parse()?;
    Ok(EndpointAddr::from_parts(id, [TransportAddr::Ip(addr)]))
}

struct Peer {
    router: Router,
    lookup: MemoryLookup,
    gossip: Gossip,
    secret: SecretKey,
    bootstrap: Vec<EndpointId>,
}

impl Peer {
    /// bind binds loopback, serves gossip, and makes the bootstrap peer's
    /// address known to the endpoint, since gossip joins by id alone.
    async fn bind(bootstrap: Option<EndpointAddr>) -> anyhow::Result<Self> {
        let secret = SecretKey::generate();
        let lookup = MemoryLookup::new();
        let ep = Endpoint::builder(presets::N0DisableRelay)
            .secret_key(secret.clone())
            .address_lookup(lookup.clone())
            // Only loopback: the builder otherwise also binds every
            // interface and announces those addresses too.
            .clear_ip_transports()
            .bind_addr("127.0.0.1:0".parse::<SocketAddr>().unwrap())?
            .bind()
            .await?;
        let addr = ep.addr();
        for ta in addr.addrs.iter() {
            if let TransportAddr::Ip(sa) = ta {
                println!("ADDR {} {}", addr.id, sa);
            }
        }
        let bootstrap = match bootstrap {
            Some(b) => {
                let id = b.id;
                lookup.add_endpoint_info(b);
                vec![id]
            }
            None => vec![],
        };
        let gossip = Gossip::builder().spawn(ep.clone());
        let router = Router::builder(ep)
            .accept(iroh_gossip::ALPN, gossip.clone())
            .spawn();
        println!("READY");
        Ok(Self { router, lookup, gossip, secret, bootstrap })
    }

    /// join parses the arguments of a "join <id> <ip:port>" command and
    /// records the address, returning the id to join.
    fn join(&self, args: &str) -> anyhow::Result<EndpointId> {
        let (id, addr) = args.split_once(' ').context("usage: join <id> <ip:port>")?;
        let addr = parse_addr(id, addr)?;
        let id = addr.id;
        self.lookup.add_endpoint_info(addr);
        Ok(id)
    }

    async fn topic(self, topic: TopicId) -> anyhow::Result<()> {
        let (sender, receiver) = self.gossip.subscribe(topic, self.bootstrap.clone()).await?.split();
        tokio::spawn(print_events(receiver));
        let mut lines = BufReader::new(tokio::io::stdin()).lines();
        while let Some(line) = lines.next_line().await? {
            if let Some(text) = line.strip_prefix("broadcast ") {
                sender.broadcast(Bytes::from(text.to_string())).await?;
            } else if let Some(rest) = line.strip_prefix("join ") {
                sender.join_peers(vec![self.join(rest)?]).await?;
            }
        }
        self.router.shutdown().await?;
        Ok(())
    }

    async fn kv(self, topic: TopicId) -> anyhow::Result<()> {
        // A second subscription to the same topic only watches membership;
        // the store owns the first.
        let (_, monitor) = self.gossip.subscribe(topic, vec![]).await?.split();
        tokio::spawn(print_events(monitor));
        let store = self.gossip.subscribe(topic, self.bootstrap.clone()).await?;
        let client = Client::local(store, Config::default());
        let writer = client.write(self.secret.clone());
        println!("SCOPE {}", writer.scope());

        let entries = client.subscribe().stream();
        tokio::spawn(async move {
            tokio::pin!(entries);
            while let Some(Ok((scope, key, value))) = entries.next().await {
                println!(
                    "ENTRY {} {} {}",
                    scope,
                    String::from_utf8_lossy(&key),
                    String::from_utf8_lossy(&value.value)
                );
            }
        });

        let mut lines = BufReader::new(tokio::io::stdin()).lines();
        while let Some(line) = lines.next_line().await? {
            if let Some(rest) = line.strip_prefix("join ") {
                client.join_peers([self.join(rest)?]).await?;
                continue;
            }
            let mut f = line.splitn(3, ' ');
            if let (Some("put"), Some(k), Some(v)) = (f.next(), f.next(), f.next()) {
                writer.put(k.to_string(), v.to_string()).await?;
            }
        }
        client.shutdown().await.ok();
        self.router.shutdown().await?;
        Ok(())
    }
}

async fn print_events(mut receiver: GossipReceiver) {
    while let Some(Ok(ev)) = receiver.next().await {
        match ev {
            Event::NeighborUp(id) => println!("UP {id}"),
            Event::NeighborDown(id) => println!("DOWN {id}"),
            Event::Received(msg) => println!(
                "RECV {} {}",
                msg.delivered_from,
                String::from_utf8_lossy(&msg.content)
            ),
            Event::Lagged => println!("LAGGED"),
        }
    }
}

/// SigningData mirrors iroh_smol_kv::proto::SigningData, which is private to
/// the crate: the postcard encoding of (key, timestamp, value) that a
/// SignedValue's signature covers. The live kv mode checks the same encoding
/// through smol-kv's own verifier.
#[derive(Serialize)]
struct SigningData<'a> {
    key: &'a [u8],
    timestamp: u64,
    value: &'a [u8],
}

/// kv_vector prints one iroh-smol-kv gossip message, encoded by smol-kv's own
/// types, from a fixed key and timestamp. Ed25519 is deterministic, so every
/// byte is reproducible.
fn kv_vector() -> anyhow::Result<()> {
    let secret = SecretKey::from_bytes(&[7u8; 32]);
    let key = Bytes::from_static(b"color");
    let value = Bytes::from_static(b"blue");
    let timestamp = 1_750_000_000_000_000_000u64;
    let signing = postcard::to_stdvec(&SigningData { key: &key, timestamp, value: &value })?;
    let signature = secret.sign(&signing).to_bytes();
    let msg = GossipMessage::SignedValue(
        secret.public(),
        key,
        SignedValue { timestamp, value, signature },
    );
    println!("SCOPE {}", secret.public());
    println!("SIGNING {}", hex::encode(signing));
    println!("MESSAGE {}", hex::encode(postcard::to_stdvec(&msg)?));
    Ok(())
}
