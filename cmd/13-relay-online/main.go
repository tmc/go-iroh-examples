package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/tmc/go-iroh/iroh"
	"github.com/tmc/go-iroh/relay"
)

func main() {
	if os.Getenv("GO_IROH_LIVE_RELAY") != "1" {
		fmt.Println("set GO_IROH_LIVE_RELAY=1 to connect to the default public relays")
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	ep, err := iroh.Bind(ctx, iroh.WithRelayMode(relay.ModeDefault()))
	if err != nil {
		panic(err)
	}
	defer ep.Shutdown(ctx)

	if err := ep.Online(ctx); err != nil {
		panic(err)
	}
	status := ep.HomeRelayStatus().Current()
	fmt.Println("endpoint id:", ep.ID().Z32())
	fmt.Println("home relay:", status.URL)
	fmt.Println("connected:", status.IsConnected())
	fmt.Println("advertised relays:", ep.Addr().RelayURLs())
}
