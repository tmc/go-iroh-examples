package main

import (
	"context"
	"fmt"
	"net/netip"
	"strings"
	"time"

	"github.com/tmc/go-iroh/iroh"
	"github.com/tmc/go-iroh/irpc"
	"github.com/tmc/go-iroh/netaddr"
)

const alpn = "go-iroh-examples/rpc-workqueue/1"

type request struct {
	ID   int
	Task string
	Body string
}

type response struct {
	ID     int
	Result string
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	server, err := iroh.Bind(ctx,
		iroh.WithBindAddr(netip.AddrPortFrom(netip.IPv6Loopback(), 0)),
		iroh.WithALPNs(alpn),
	)
	if err != nil {
		panic(err)
	}
	defer server.Shutdown(ctx)

	go func() {
		conn, err := server.Accept(ctx)
		if err != nil {
			return
		}
		handler := irpc.Handler[request, response]{
			Handle: func(ctx context.Context, req request, r *irpc.Responder[response]) error {
				return r.Send(response{ID: req.ID, Result: runTask(req)})
			},
		}
		_ = handler.Accept(ctx, conn)
	}()

	client, err := iroh.Bind(ctx, iroh.WithBindAddr(netip.AddrPortFrom(netip.IPv6Loopback(), 0)))
	if err != nil {
		panic(err)
	}
	defer client.Shutdown(ctx)

	addr := netaddr.NewEndpointAddr(server.ID()).WithIP(server.LocalAddr())
	conn, err := client.Connect(ctx, addr, alpn)
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	jobs := []request{
		{ID: 1, Task: "upper", Body: "first job"},
		{ID: 2, Task: "reverse", Body: "second job"},
		{ID: 3, Task: "count", Body: "third job"},
	}
	results := make(chan response, len(jobs))
	for _, job := range jobs {
		go func() {
			resp, err := call(ctx, conn, job)
			if err != nil {
				panic(err)
			}
			results <- resp
		}()
	}

	for range jobs {
		resp := <-results
		fmt.Printf("job %d: %s\n", resp.ID, resp.Result)
	}
}

func call(ctx context.Context, conn *iroh.Conn, req request) (response, error) {
	responses, err := irpc.Call[request, response](ctx, conn, req, 0)
	if err != nil {
		return response{}, err
	}
	for resp, err := range responses {
		if err != nil {
			return response{}, err
		}
		return resp, nil
	}
	return response{}, fmt.Errorf("missing response")
}

func runTask(req request) string {
	switch req.Task {
	case "upper":
		return strings.ToUpper(req.Body)
	case "reverse":
		r := []rune(req.Body)
		for i, j := 0, len(r)-1; i < j; i, j = i+1, j-1 {
			r[i], r[j] = r[j], r[i]
		}
		return string(r)
	case "count":
		return fmt.Sprintf("%d bytes", len(req.Body))
	default:
		return "unknown task"
	}
}
