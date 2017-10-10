package store

import (
	"testing"
)

func Test_ClusterMeta(t *testing.T) {
	c := newClusterMeta()
	c.APIPeers["localhost:4002"] = "localhost:4001"

	if c.AddrForPeer("localhost:4002") != "localhost:4001" {
		t.Fatalf("wrong address returned for localhost:4002")
	}

	if c.AddrForPeer("127.0.0.1:4002") != "localhost:4001" {
		t.Fatalf("wrong address returned for 127.0.0.1:4002")
	}

	if c.AddrForPeer("127.0.0.1:4004") != "" {
		t.Fatalf("wrong address returned for 127.0.0.1:4003")
	}
}
