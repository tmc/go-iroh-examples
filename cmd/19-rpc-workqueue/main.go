// Command 19-rpc-workqueue dispatches concurrent RPC jobs over one connection.
//
// [irpc] is the smallest thing on top of iroh that behaves like RPC: a request
// and its responses are postcard-encoded and length-framed on one QUIC stream.
// [irpc.Call] opens the stream, sends the typed request, and returns an
// iterator over the typed responses. [irpc.Handler] is the other half: it
// decodes the request and hands application code an [irpc.Responder].
//
// One stream per call is what makes a work queue out of it. The three jobs here
// are dispatched at the same time on a single connection, each on its own
// stream, so a slow job does not delay the ones behind it and there is no need
// for request IDs or a multiplexing layer of one's own. Results are printed as
// they finish, not in submission order.
//
// A responder may send more than one response, which is how a long job reports
// progress: each r.Send is another value from the caller's iterator, and the
// iterator ends when the handler returns. This example sends exactly one
// response per job, the ordinary request/response case.
//
// Choose this over framing messages by hand — 41-framed-messages — when the
// payloads are Go values and a request maps to a response. Choose a stream when
// the payload is bytes with no natural message boundary.
package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/tmc/go-iroh-examples/internal/exampleutil"
	"github.com/tmc/go-iroh/iroh"
	"github.com/tmc/go-iroh/irpc"
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

// result pairs a job's response with the error from its call, so that a
// goroutine reports a failure instead of ending the process.
type result struct {
	id   int
	resp response
	err  error
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	server, err := exampleutil.Bind(ctx, iroh.WithALPNs(alpn))
	if err != nil {
		return err
	}
	defer server.Shutdown(ctx)

	// The handler runs until the client closes the connection, which happens
	// after run has printed everything, so only a failure is worth reporting.
	serverErr := make(chan error, 1)
	go func() {
		if err := serve(ctx, server); err != nil {
			serverErr <- err
		}
	}()

	client, err := exampleutil.Bind(ctx)
	if err != nil {
		return err
	}
	defer client.Shutdown(ctx)

	conn, err := client.Connect(ctx, exampleutil.Addr(server), alpn)
	if err != nil {
		return err
	}
	defer conn.Close()

	jobs := []request{
		{ID: 1, Task: "upper", Body: "first job"},
		{ID: 2, Task: "reverse", Body: "second job"},
		{ID: 3, Task: "count", Body: "third job"},
	}
	results := make(chan result, len(jobs))
	for _, job := range jobs {
		go func() {
			resp, err := call(ctx, conn, job)
			results <- result{id: job.ID, resp: resp, err: err}
		}()
	}

	for range jobs {
		select {
		case r := <-results:
			if r.err != nil {
				return fmt.Errorf("job %d: %w", r.id, r.err)
			}
			fmt.Printf("job %d: %s\n", r.resp.ID, r.resp.Result)
		case err := <-serverErr:
			return fmt.Errorf("serve: %w", err)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

// serve answers requests on one connection until the peer closes it.
func serve(ctx context.Context, ep *iroh.Endpoint) error {
	conn, err := ep.Accept(ctx)
	if err != nil {
		return err
	}
	handler := irpc.Handler[request, response]{
		Handle: func(ctx context.Context, req request, r *irpc.Responder[response]) error {
			return r.Send(response{ID: req.ID, Result: runTask(req)})
		},
	}
	return handler.Accept(ctx, conn)
}

// call performs one request and returns its first response. A handler that
// streams would iterate the sequence instead of stopping at the first value.
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
	return response{}, fmt.Errorf("no response for job %d", req.ID)
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
