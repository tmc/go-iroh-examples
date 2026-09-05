//! Ground-truth wire vectors, produced by the Rust implementation.
//!
//! go-iroh-examples pins these bytes in a Go test so that a framing change on
//! either side fails loudly instead of silently ending interoperability.

use framed_messages::Move;

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

/// One framed message exactly as it goes on the wire: a big-endian u32 length
/// prefix followed by the postcard encoding of the move.
fn frame(mv: &Move) -> Vec<u8> {
    let body = postcard::to_allocvec(mv).expect("encode");
    let mut out = (body.len() as u32).to_be_bytes().to_vec();
    out.extend_from_slice(&body);
    out
}

fn main() {
    println!("alpn\t{}", hex(framed_messages::ALPN));
    for (name, mv) in [
        ("white_e2e4", Move { from: (4, 2), to: (4, 4) }),
        ("white_d2d3", Move { from: (3, 2), to: (3, 3) }),
        ("black_f7f6", Move { from: (5, 7), to: (5, 6) }),
        ("black_f8f7", Move { from: (5, 8), to: (5, 7) }),
    ] {
        println!("move_{name}\t{}", hex(&postcard::to_allocvec(&mv).expect("encode")));
        println!("frame_{name}\t{}", hex(&frame(&mv)));
    }
}
