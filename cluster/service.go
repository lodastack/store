package cluster

import (
	"net"
	"sync"
	"time"

	"github.com/lodastack/log"
	"github.com/lodastack/store/model"
)

const (
	connectionTimeout = 10 * time.Second
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
func NewService(tn Transport, s Store) *Service {
	return &Service{
		tn:     tn,
		store:  s,
		addr:   tn.Addr(),
		logger: log.New("INFO", "cluster", model.LogBackend),
	}
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

// Addr returns the address the service is listening on.
func (s *Service) Addr() string {
	return s.addr.String()
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
