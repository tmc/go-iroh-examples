package main

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strings"

	"github.com/tmc/go-iroh/dns"
	"github.com/tmc/go-iroh/dnsserver"
	"github.com/tmc/go-iroh/key"
	"github.com/tmc/go-iroh/metrics"
	"github.com/tmc/go-iroh/netaddr"
	"github.com/tmc/go-iroh/relayserver"
	"golang.org/x/net/dns/dnsmessage"
)

func main() {
	sk, err := key.ParseSecretKey("vpnk377obfvzlipnsfbqba7ywkkenc4xlpmovt5tsfujoa75zqia")
	if err != nil {
		panic(err)
	}
	userData, err := dns.NewUserData("local-infra")
	if err != nil {
		panic(err)
	}

	info := dns.EndpointInfo{ID: sk.Public().EndpointID()}
	info.Data.AddIPAddrs(netip.MustParseAddrPort("127.0.0.1:4433"))
	info.Data.SetUserData(&userData)
	packet, err := info.ToSignedPacket(sk, 30)
	if err != nil {
		panic(err)
	}

	dnsSrv := dnsserver.New()
	dnsHTTP := httptest.NewServer(dnsSrv)
	defer dnsHTTP.Close()

	keyLabel := info.ID.Z32()
	req, err := http.NewRequest(http.MethodPut, dnsHTTP.URL+"/pkarr/"+keyLabel, bytes.NewReader(packet.RelayPayload()))
	if err != nil {
		panic(err)
	}
	resp, err := dnsHTTP.Client().Do(req)
	if err != nil {
		panic(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		panic(fmt.Sprintf("publish status %d", resp.StatusCode))
	}

	resp, err = dnsHTTP.Client().Get(dnsHTTP.URL + "/pkarr/" + keyLabel)
	if err != nil {
		panic(err)
	}
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		panic(err)
	}
	fmt.Println("pkarr payload bytes:", len(body))

	txt, err := queryTXT(dnsSrv, dns.IrohTXTName+"."+keyLabel+".example.")
	if err != nil {
		panic(err)
	}
	fmt.Println("dns txt:", strings.Join(txt, ", "))

	relaySrv := relayserver.New()
	relayHTTP := httptest.NewServer(relaySrv)
	defer relayHTTP.Close()
	resp, err = relayHTTP.Client().Get(relayHTTP.URL + "/")
	if err != nil {
		panic(err)
	}
	resp.Body.Close()
	fmt.Println("relay root status:", resp.StatusCode)

	reg := metrics.NewRegistry()
	if err := reg.Register("dnsserver", dnsSrv); err != nil {
		panic(err)
	}
	if err := reg.Register("relayserver", relaySrv); err != nil {
		panic(err)
	}
	if err := reg.WriteOpenMetrics(io.Discard); err != nil {
		panic(err)
	}
	fmt.Println("dns snapshot:", dnsSrv.Snapshot())
	fmt.Println("relay snapshot:", relaySrv.Snapshot())

	addr := netaddr.NewEndpointAddr(info.ID, info.Data.Addrs()...)
	fmt.Println("endpoint addrs:", len(addr.Addrs()))
}

func queryTXT(srv *dnsserver.Server, name string) ([]string, error) {
	dnsName, err := dnsmessage.NewName(name)
	if err != nil {
		return nil, err
	}
	b := dnsmessage.NewBuilder(nil, dnsmessage.Header{ID: 1, RecursionDesired: true})
	if err := b.StartQuestions(); err != nil {
		return nil, err
	}
	if err := b.Question(dnsmessage.Question{
		Name:  dnsName,
		Type:  dnsmessage.TypeTXT,
		Class: dnsmessage.ClassINET,
	}); err != nil {
		return nil, err
	}
	msg, err := b.Finish()
	if err != nil {
		return nil, err
	}
	resp, err := srv.ServeDNSPacket(msg)
	if err != nil {
		return nil, err
	}
	return readTXT(resp)
}

func readTXT(msg []byte) ([]string, error) {
	var p dnsmessage.Parser
	h, err := p.Start(msg)
	if err != nil {
		return nil, err
	}
	if !h.Response {
		return nil, fmt.Errorf("not a response")
	}
	if err := p.SkipAllQuestions(); err != nil {
		return nil, err
	}
	var out []string
	for {
		h, err := p.AnswerHeader()
		if err == dnsmessage.ErrSectionDone {
			return out, nil
		}
		if err != nil {
			return nil, err
		}
		if h.Type != dnsmessage.TypeTXT {
			if err := p.SkipAnswer(); err != nil {
				return nil, err
			}
			continue
		}
		txt, err := p.TXTResource()
		if err != nil {
			return nil, err
		}
		out = append(out, txt.TXT...)
	}
}
