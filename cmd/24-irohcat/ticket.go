package main

import (
	"github.com/tmc/go-iroh/endpointticket"
	"github.com/tmc/go-iroh/netaddr"
)

func encodeEndpointTicket(addr netaddr.EndpointAddr) string {
	return endpointticket.Encode(addr)
}

func decodeEndpointTicket(ticket string) (netaddr.EndpointAddr, error) {
	return endpointticket.Decode(ticket)
}
