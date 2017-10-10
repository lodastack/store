package store

import (
	"net"
)

// clusterMeta represents cluster meta which must be kept in consensus.
type clusterMeta struct {
	APIPeers map[string]string // Map from Raft address to API address
}

// NewClusterMeta returns an initialized cluster meta store.
func newClusterMeta() *clusterMeta {
	return &clusterMeta{
		APIPeers: make(map[string]string),
	}
}

func (c *clusterMeta) AddrForPeer(addr string) string {
	if api, ok := c.APIPeers[addr]; ok && api != "" {
		return api
	}

	// Go through each entry, and see if any key resolves to addr.
	for k, v := range c.APIPeers {
		resv, err := net.ResolveTCPAddr("tcp", k)
		if err != nil {
			continue
		}
		if resv.String() == addr {
			return v
		}
	}

	return ""
}
