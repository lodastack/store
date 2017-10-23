package cluster

import (
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/lodastack/log"
	"github.com/lodastack/store/model"
	"github.com/lodastack/store/store"
	"github.com/lodastack/store/tcp"
)

const (
	connectionTimeout = 10 * time.Second
)

const (
	muxRaftHeader = 1 // Raft consensus communications
	muxMetaHeader = 2 // Cluster meta communications
)

// Transport is the interface the network service must provide.
type Transport interface {
	net.Listener

	// Dial is used to create a new outgoing connection.
	Dial(address string, timeout time.Duration) (net.Conn, error)
}

// Store represents a store of information, managed via consensus.
type Store interface {
	// Leader returns the leader of the consensus system.
	Leader() string

	// Join joins the node, reachable at addr, to the cluster.
	Join(addr string) error

	// Remove removes a node from the store, specified by addr.
	Remove(addr string) error

	// UpdateAPIPeers updates the API peers on the store.
	UpdateAPIPeers(peers map[string]string) error

	// Create a bucket, via distributed consensus.
	CreateBucket(name []byte) error

	// Create a bucket via distributed consensus if not exist.
	CreateBucketIfNotExist(name []byte) error

	// Remove a bucket, via distributed consensus.
	RemoveBucket(name []byte) error

	// Get returns the value for the given key.
	View(bucket, key []byte) ([]byte, error)

	// ViewPrefix returns the value for the keys has the keyPrefix.
	ViewPrefix(bucket, keyPrefix []byte) (map[string][]byte, error)

	// Set sets the value for the given key, via distributed consensus.
	Update(bucket []byte, key []byte, value []byte) error

	// RemoveKey removes the key from the bucket.
	RemoveKey(bucket, key []byte) error

	// Batch update values for given keys in given buckets, via distributed consensus.
	Batch(rows []model.Row) error

	// GetSession returns the sression value for the given key.
	GetSession(key interface{}) interface{}

	// SetSession sets the value for the given key, via distributed consensus.
	SetSession(key, value interface{}) error

	// DelSession delete the value for the given key, via distributed consensus.
	DelSession(key interface{}) error

	// Backup database.
	Backup() ([]byte, error)

	// Restore restores backup data file.
	Restore(backupfile string) error

	// APIPeers return the map of Raft addresses to API addresses.
	APIPeers() (map[string]string, error)

	// Nodes returns the list of current peers.
	Nodes() ([]string, error)

	// WaitForLeader blocks until a leader is detected, or the timeout expires.
	WaitForLeader(timeout time.Duration) (string, error)

	// Close closes the store. If wait is true, waits for a graceful shutdown.
	Close(wait bool) error

	// Statistics returns statistics for periodic monitoring.
	Statistics(tags map[string]string) []model.Statistic
}

// Service allows access to the cluster and associated meta data,
// via distributed consensus.
type Service struct {
	tn    Transport
	store Store
	addr  net.Addr

	wg   sync.WaitGroup
	done chan struct{} // Is the service closing or closed?

	logger *log.Logger
}

// NewService returns a new instance of the cluster service.
func NewService(bind string, dir string, joinAddr string) (*Service, error) {
	// serve mux TCP
	ln, err := net.Listen("tcp", bind)
	if err != nil {
		return nil, fmt.Errorf("failed to listen on %s: %s", bind, err.Error())
	}
	mux := tcp.NewMux(ln, nil)
	go mux.Serve()

	// Start up mux and get transports for cluster.
	raftTn := mux.Listen(muxRaftHeader)
	s := store.New(dir, raftTn)
	if err := s.Open(joinAddr == ""); err != nil {
		return nil, fmt.Errorf("failed to open store: %s", err.Error())
	}

	// Create and configure cluster service.
	tn := mux.Listen(muxMetaHeader)

	return &Service{
		tn:     tn,
		store:  s,
		addr:   tn.Addr(),
		logger: log.New("INFO", "cluster", model.LogBackend),
	}, nil
}

// Open opens the Service.
func (s *Service) Open() error {
	if !s.closed() {
		return nil // Already open.
	}
	s.done = make(chan struct{})

	go s.serve()
	s.logger.Println("service listening on", s.tn.Addr())
	return nil
}

// Close closes the service.
func (s *Service) Close() error {
	if s.closed() {
		return nil // Already closed.
	}
	if err := s.store.Close(true); err != nil {
		return err
	}
	close(s.done)
	s.tn.Close()
	s.wg.Wait()
	return nil
}

func (s *Service) closed() bool {
	select {
	case <-s.done:
		// Service is closing.
		return true
	default:
	}
	return s.done == nil
}

// Statistics returns statistics for periodic monitoring.
func (s *Service) Statistics(tags map[string]string) []model.Statistic {
	return s.store.Statistics(tags)
}

// Addr returns the address the service is listening on.
func (s *Service) Addr() string {
	return s.addr.String()
}

// JoinCluster joins the cluster via TCP, reachable at addr, to the cluster.
func (s *Service) JoinCluster(server, addr string) error {
	return s.Write(server, map[string][]byte{
		"addr": []byte(addr),
		"type": TypJoin,
	})
}

// PublishAPIAddr public API addr in cluster
func (s *Service) PublishAPIAddr(apiAddr string, delay time.Duration, timeout time.Duration) error {
	tck := time.NewTicker(delay)
	defer tck.Stop()
	tmr := time.NewTimer(timeout)
	defer tmr.Stop()

	for {
		select {
		case <-tck.C:
			if err := s.SetPeer(s.Addr(), apiAddr); err != nil {
				log.Errorf("failed to set peer for %s to %s: %s (retrying)",
					s.Addr(), apiAddr, err.Error())
				continue
			}
			return nil
		case <-tmr.C:
			return fmt.Errorf("set peer timeout expired")
		}
	}
}

func (s *Service) serve() error {
	for {
		select {
		case <-s.done:
			// We closed the connection, time to go.
			return nil
		default:
			conn, err := s.tn.Accept()
			if err != nil {
				s.logger.Errorf("accept error: %s", err.Error())
			}
			s.wg.Add(1)
			go s.handleConn(conn)
		}
	}
}
