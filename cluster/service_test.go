package cluster_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/lodastack/store/cluster"
)

func Example() {
	opts := cluster.Options{
		// store bind TCP listen
		Bind: "127.0.0.1:9000",
		// store data dir
		DataDir: "/tmp/store",
		// any node in exist cluster
		JoinAddr: "10.0.0.1:9000",
	}

	cs, err := cluster.NewService(opts)
	if err != nil {
		fmt.Printf("new store service failed: %s", err.Error())
	}

	if err := cs.Open(); err != nil {
		fmt.Printf("failed to open cluster service failed: %s", err.Error())
	}

	// If join was specified, make the join request.
	nodes, err := cs.Nodes()
	if err != nil {
		fmt.Printf("get nodes failed: %s", err.Error())
	}

	// if exist a raftdb, or exist a cluster, don't join any leader.
	if opts.JoinAddr != "" && len(nodes) <= 1 {
		if err := cs.JoinCluster(opts.JoinAddr, opts.Bind); err != nil {
			fmt.Printf("failed to join node at %s: %s", opts.JoinAddr, err.Error())
		}
	}
}

func Test_NewService_leader(t *testing.T) {
	opts := cluster.Options{
		// store bind TCP listen
		Bind: "127.0.0.1:9000",
		// store data dir
		DataDir: "/tmp/store",
	}

	cs, err := cluster.NewService(opts)
	if err != nil {
		t.Fatalf("new store service failed: %s", err.Error())
	}

	if err := cs.Open(); err != nil {
		t.Fatalf("failed to open cluster service failed: %s", err.Error())
	}

	// If join was specified, make the join request.
	_, err = cs.Nodes()
	if err != nil {
		t.Fatalf("get nodes failed: %s", err.Error())
	}
	cs.Close()
}

func Test_NewService_follower(t *testing.T) {
	opts1 := cluster.Options{
		// store bind TCP listen
		Bind: "127.0.0.1:8000",
		// store data dir
		DataDir: "/tmp/store1",
	}

	cs1, err := cluster.NewService(opts1)
	if err != nil {
		t.Fatalf("new store service failed: %s", err)
	}

	if err := cs1.Open(); err != nil {
		t.Fatalf("failed to open cluster service failed: %s", err)
	}

	// If join was specified, make the join request.
	_, err = cs1.Nodes()
	if err != nil {
		t.Fatalf("get nodes failed: %s", err)
	}

	opts2 := cluster.Options{
		// store bind TCP listen
		Bind: "127.0.0.1:8001",
		// store data dir
		DataDir: "/tmp/store2",
		// any node in exist cluster
		JoinAddr: "127.0.0.1:8000",
	}

	cs2, err := cluster.NewService(opts2)
	if err != nil {
		t.Fatalf("new store service failed: %s", err)
	}

	if err := cs2.Open(); err != nil {
		t.Fatalf("failed to open cluster service failed: %s", err)
	}

	// If join was specified, make the join request.
	nodes, err := cs2.Nodes()
	if err != nil {
		t.Fatalf("get nodes failed: %s", err)
	}

	time.Sleep(5 * time.Second)

	// if exist a raftdb, or exist a cluster, don't join any leader.
	if opts2.JoinAddr != "" && len(nodes) <= 1 {
		if err := cs2.JoinCluster(opts2.JoinAddr, opts2.Bind); err != nil {
			t.Fatalf("failed to join node at %s: %s", opts2.JoinAddr, err)
		}
	}
	cs2.Close()
	cs1.Close()
}
