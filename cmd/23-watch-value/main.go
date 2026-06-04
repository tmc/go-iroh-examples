package main

import (
	"context"
	"fmt"
	"time"

	"github.com/tmc/go-iroh/watch"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	value := watch.NewValue("starting")
	obs := value.Watch()
	fmt.Println("current:", obs.Current())

	updated := make(chan error, 1)
	go func() {
		next, err := obs.Updated(ctx)
		if err != nil {
			updated <- err
			return
		}
		fmt.Println("updated:", next)
		updated <- nil
	}()
	value.Set("ready")
	if err := <-updated; err != nil {
		panic(err)
	}

	unique := watch.NewValueFunc(0, func(a, b int) bool { return a == b })
	streamCtx, stopStream := context.WithCancel(ctx)
	defer stopStream()
	seen := 0
	for n := range unique.Watch().Stream(streamCtx) {
		fmt.Println("stream:", n)
		seen++
		if seen == 3 {
			break
		}
		switch n {
		case 0:
			unique.Set(1)
		case 1:
			unique.Set(1)
			unique.Set(2)
		}
	}
}
