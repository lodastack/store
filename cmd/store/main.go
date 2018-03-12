package main

import (
	"fmt"

	"github.com/lodastack/store/cluster"
)

func main() {
	opts := cluster.Options{
		// store bind TCP listen
		Bind: "127.0.0.1:9000",
		// store data dir
		DataDir: "/tmp/store",
	}

	cs, err := cluster.NewService(opts)
	if err != nil {
		fmt.Printf("new store service failed: %s", err)
		return
	}

	if err := cs.Open(); err != nil {
		fmt.Printf("failed to open cluster service failed: %s", err)
		return
	}

	// If join was specified, make the join request.
	nodes, err := cs.Nodes()
	if err != nil {
		fmt.Printf("get nodes failed: %s", err)
		return
	}

	fmt.Println(nodes)
}
