package dumbticket

import (
	"encoding/base32"
	"errors"
	"fmt"
	"net/netip"
	"slices"
	"strings"

	"github.com/tmc/go-iroh/key"
	"github.com/tmc/go-iroh/netaddr"
)

var base32NoPad = base32.StdEncoding.WithPadding(base32.NoPadding)

// EncodeEndpoint encodes addr as a Rust iroh-tickets endpoint ticket.
func EncodeEndpoint(addr netaddr.EndpointAddr) string {
	var b []byte
	b = append(b, 0) // TicketWireFormat::Variant1.
	id := addr.ID.Bytes()
	b = append(b, id[:]...)
	addrs := addr.Addrs()
	b = appendVarint(b, uint64(len(addrs)))
	for _, a := range addrs {
		b = appendTransportAddr(b, a)
	}
	out := "endpoint" + base32NoPad.EncodeToString(b)
	return strings.ToLower(out)
}

// DecodeEndpoint decodes a Rust iroh-tickets endpoint ticket.
func DecodeEndpoint(ticket string) (netaddr.EndpointAddr, error) {
	rest, ok := strings.CutPrefix(ticket, "endpoint")
	if !ok {
		return netaddr.EndpointAddr{}, errors.New("endpoint ticket: missing endpoint prefix")
	}
	b, err := base32NoPad.DecodeString(strings.ToUpper(rest))
	if err != nil {
		return netaddr.EndpointAddr{}, fmt.Errorf("endpoint ticket: decode base32: %w", err)
	}
	p := parser{b: b}
	variant, err := p.varint()
	if err != nil {
		return netaddr.EndpointAddr{}, err
	}
	if variant != 0 {
		return netaddr.EndpointAddr{}, fmt.Errorf("endpoint ticket: unsupported variant %d", variant)
	}
	idBytes, err := p.bytes(key.PublicKeySize)
	if err != nil {
		return netaddr.EndpointAddr{}, err
	}
	id, err := key.EndpointIDFromSlice(idBytes)
	if err != nil {
		return netaddr.EndpointAddr{}, fmt.Errorf("endpoint ticket: endpoint id: %w", err)
	}
	n, err := p.varint()
	if err != nil {
		return netaddr.EndpointAddr{}, err
	}
	if n > 1024 {
		return netaddr.EndpointAddr{}, fmt.Errorf("endpoint ticket: too many addresses %d", n)
	}
	addrs := make([]netaddr.TransportAddr, 0, n)
	for range n {
		a, err := p.transportAddr()
		if err != nil {
			return netaddr.EndpointAddr{}, err
		}
		addrs = append(addrs, a)
	}
	if !p.done() {
		return netaddr.EndpointAddr{}, errors.New("endpoint ticket: trailing bytes")
	}
	return netaddr.NewEndpointAddr(id, addrs...), nil
}

func appendTransportAddr(b []byte, a netaddr.TransportAddr) []byte {
	switch a := a.(type) {
	case netaddr.RelayAddr:
		b = append(b, 0)
		s := []byte(a.URL.String())
		b = appendVarint(b, uint64(len(s)))
		return append(b, s...)
	case netaddr.IPAddr:
		b = append(b, 1)
		ap := a.Addr
		if ap.Addr().Is4() {
			ip4 := ap.Addr().As4()
			b = append(b, 0)
			b = append(b, ip4[:]...)
		} else {
			ip6 := ap.Addr().As16()
			b = append(b, 1)
			b = append(b, ip6[:]...)
		}
		return appendVarint(b, uint64(ap.Port()))
	case netaddr.CustomAddr:
		b = append(b, 2)
		b = appendVarint(b, a.ID())
		data := a.Data()
		b = appendVarint(b, uint64(len(data)))
		return append(b, data...)
	default:
		panic("unknown transport address")
	}
}

func appendVarint(b []byte, n uint64) []byte {
	for n >= 0x80 {
		b = append(b, byte(n)|0x80)
		n >>= 7
	}
	return append(b, byte(n))
}

type parser struct {
	b   []byte
	off int
}

func (p *parser) done() bool { return p.off == len(p.b) }

func (p *parser) bytes(n int) ([]byte, error) {
	if n < 0 || len(p.b)-p.off < n {
		return nil, errors.New("endpoint ticket: truncated")
	}
	out := p.b[p.off : p.off+n]
	p.off += n
	return out, nil
}

func (p *parser) varint() (uint64, error) {
	var n uint64
	for shift := uint(0); shift < 64; shift += 7 {
		b, err := p.bytes(1)
		if err != nil {
			return 0, err
		}
		n |= uint64(b[0]&0x7f) << shift
		if b[0]&0x80 == 0 {
			return n, nil
		}
	}
	return 0, errors.New("endpoint ticket: varint overflow")
}

func (p *parser) transportAddr() (netaddr.TransportAddr, error) {
	kind, err := p.varint()
	if err != nil {
		return nil, err
	}
	switch kind {
	case 0:
		n, err := p.varint()
		if err != nil {
			return nil, err
		}
		s, err := p.bytes(int(n))
		if err != nil {
			return nil, err
		}
		u, err := netaddr.ParseRelayURL(string(s))
		if err != nil {
			return nil, err
		}
		return netaddr.RelayAddr{URL: u}, nil
	case 1:
		family, err := p.varint()
		if err != nil {
			return nil, err
		}
		var ip netip.Addr
		switch family {
		case 0:
			b, err := p.bytes(4)
			if err != nil {
				return nil, err
			}
			ip = netip.AddrFrom4([4]byte(b))
		case 1:
			b, err := p.bytes(16)
			if err != nil {
				return nil, err
			}
			ip = netip.AddrFrom16([16]byte(b))
		default:
			return nil, fmt.Errorf("endpoint ticket: unsupported IP family %d", family)
		}
		port, err := p.varint()
		if err != nil {
			return nil, err
		}
		if port > 65535 {
			return nil, fmt.Errorf("endpoint ticket: invalid port %d", port)
		}
		return netaddr.IPAddr{Addr: netip.AddrPortFrom(ip, uint16(port))}, nil
	case 2:
		id, err := p.varint()
		if err != nil {
			return nil, err
		}
		n, err := p.varint()
		if err != nil {
			return nil, err
		}
		data, err := p.bytes(int(n))
		if err != nil {
			return nil, err
		}
		return netaddr.NewCustomAddr(id, slices.Clone(data)), nil
	default:
		return nil, fmt.Errorf("endpoint ticket: unsupported transport kind %d", kind)
	}
}
