package cluster

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net"
	"time"

	"github.com/lodastack/store/model"

	"github.com/hashicorp/raft"
)

var errNotLeader = raft.ErrNotLeader

var (
	typCBucket         = []byte("createrBucket")
	typRBucket         = []byte("removeBucket")
	typUpdate          = []byte("update")
	typBatch           = []byte("batch")
	typCBucketNotExist = []byte("createBucketIfNotExist")
	typRkey            = []byte("removekey")

	typSetSession = []byte("setsession")
	typDelSession = []byte("delsession")

	typJoin   = []byte("join")
	typRemove = []byte("remove")
	typPeer   = []byte("peer")

	leader   = "Leader"
	follower = "Follower"
)

type response struct {
	Code    int    `json:"code,omitempty"`
	Message string `json:"message,omitempty"`
}

// SetPeer will set the mapping between raftAddr and apiAddr for the entire cluster.
func (s *Service) SetPeer(raftAddr, apiAddr string) error {
	// Try the local store. It might be the leader.
	err := s.store.UpdateAPIPeers(map[string]string{raftAddr: apiAddr})
	if err == errNotLeader {
		return s.writeLeader(map[string][]byte{
			"api":  []byte(apiAddr),
			"raft": []byte(raftAddr),
			"type": typPeer,
		})
	}
	return err
}

// Join joins the node, reachable at addr, to the cluster.
func (s *Service) Join(addr string) error {
	// Try the local store. It might be the leader.
	err := s.store.Join(addr)
	if err == errNotLeader {
		return s.writeLeader(map[string][]byte{
			"addr": []byte(addr),
			"type": typJoin,
		})
	}
	return err
}

// Peers returns cluster roles.
func (s *Service) Peers() (map[string]map[string]string, error) {
	peerMap := make(map[string]map[string]string)
	peers, err := s.store.APIPeers()
	if err != nil {
		return nil, err
	}
	Leadership := s.store.Leader()

	for raftAddr, apiAddr := range peers {
		peerMap[raftAddr] = make(map[string]string)
		peerMap[raftAddr]["api"] = apiAddr
		if raftAddr == Leadership {
			peerMap[raftAddr]["role"] = leader
		} else {
			peerMap[raftAddr]["role"] = follower
		}
	}
	return peerMap, nil
}

// Nodes returns the list of current peers.
func (s *Service) Nodes() ([]string, error) {
	return s.store.Nodes()
}

// WaitForLeader blocks until a leader is detected, or the timeout expires.
func (s *Service) WaitForLeader(timeout time.Duration) (string, error) {
	return s.store.WaitForLeader(timeout)
}

// Remove removes a node from the store, specified by addr.
func (s *Service) Remove(addr string) error {
	// Try the local store. It might be the leader.
	err := s.store.Remove(addr)
	if err == errNotLeader {
		return s.writeLeader(map[string][]byte{
			"addr": []byte(addr),
			"type": typRemove,
		})
	}
	return err
}

// CreateBucket will create bucket via the cluster.
func (s *Service) CreateBucket(name []byte) error {
	// Try the local store. It might be the leader.
	err := s.store.CreateBucket(name)
	if err == errNotLeader {
		return s.writeLeader(map[string][]byte{
			"name": name,
			"type": typCBucket,
		})
	}
	return err
}

// CreateBucketIfNotExist will create bucket via the cluster if not exist.
func (s *Service) CreateBucketIfNotExist(name []byte) error {
	// Try the local store. It might be the leader.
	err := s.store.CreateBucketIfNotExist(name)
	if err == errNotLeader {
		return s.writeLeader(map[string][]byte{
			"name": name,
			"type": typCBucketNotExist,
		})
	}
	return err
}

// RemoveBucket will remove bucket via the cluster.
func (s *Service) RemoveBucket(name []byte) error {
	// Try the local store. It might be the leader.
	err := s.store.RemoveBucket(name)
	if err == errNotLeader {
		return s.writeLeader(map[string][]byte{
			"name": name,
			"type": typRBucket,
		})
	}
	return err
}

// View returns the value for the given key.
func (s *Service) View(bucket, key []byte) ([]byte, error) {
	return s.store.View(bucket, key)
}

// ViewPrefix returns the value for the keys has the keyPrefix.
func (s *Service) ViewPrefix(bucket, keyPrefix []byte) (map[string][]byte, error) {
	return s.store.ViewPrefix(bucket, keyPrefix)
}

// RemoveKey removes the key from the bucket.
func (s *Service) RemoveKey(bucket, key []byte) error {
	// Try the local store. It might be the leader.
	err := s.store.RemoveKey(bucket, key)
	if err == errNotLeader {
		return s.writeLeader(map[string][]byte{
			"key":    key,
			"bucket": bucket,
			"type":   typRkey,
		})
	}
	return err
}

// Update will update the value of the given key in bucket via the cluster.
func (s *Service) Update(bucket []byte, key []byte, value []byte) error {
	// Try the local store. It might be the leader.
	err := s.store.Update(bucket, key, value)
	if err == errNotLeader {
		return s.writeLeader(map[string][]byte{
			"key":    key,
			"value":  value,
			"bucket": bucket,
			"type":   typUpdate,
		})
	}
	return err
}

// Batch update values for given keys in given buckets, via distributed consensus.
func (s *Service) Batch(rows []model.Row) error {
	// Try the local store. It might be the leader.
	err := s.store.Batch(rows)
	if err == errNotLeader {
		// Don't use binary to encode?
		// https://github.com/golang/go/issues/478
		buf := &bytes.Buffer{}
		e := json.NewEncoder(buf)
		if err := e.Encode(rows); err != nil {
			return err
		}

		return s.writeLeader(map[string][]byte{
			"rows": buf.Bytes(),
			"type": typBatch,
		})
	}
	return err
}

// GetSession returns the session value for the given key.
func (s *Service) GetSession(key interface{}) interface{} {
	return s.store.GetSession(key)
}

// SetSession set the session.
func (s *Service) SetSession(key, value interface{}) error {
	// Try the local store. It might be the leader.
	err := s.store.SetSession(key, value)
	if err == errNotLeader {
		var keyStr, valueStr string
		var ok bool
		if keyStr, ok = key.(string); !ok {
			return fmt.Errorf("session key type error, not a string")
		}
		if valueStr, ok = value.(string); !ok {
			return fmt.Errorf("session value type error, not a string")
		}
		return s.writeLeader(map[string][]byte{
			"key":   []byte(keyStr),
			"value": []byte(valueStr),
			"type":  typSetSession,
		})
	}
	return err
}

// DelSession delete the session from given key.
func (s *Service) DelSession(key interface{}) error {
	// Try the local store. It might be the leader.
	err := s.store.DelSession(key)
	if err == errNotLeader {
		var keyStr string
		var ok bool
		if keyStr, ok = key.(string); !ok {
			return fmt.Errorf("session key type error, not a string")
		}
		return s.writeLeader(map[string][]byte{
			"key":  []byte(keyStr),
			"type": typDelSession,
		})
	}
	return err
}

// Backup database.
func (s *Service) Backup() ([]byte, error) {
	return s.store.Backup()
}

// Restore database.
func (s *Service) Restore(Backupfile string) error {
	return s.store.Restore(Backupfile)
}

func (s *Service) writeLeader(msg interface{}) error {
	// Try talking to the leader over the network.
	if leader := s.store.Leader(); leader == "" {
		return fmt.Errorf("no leader available")
	}
	conn, err := s.tn.Dial(s.store.Leader(), connectionTimeout)
	if err != nil {
		return err
	}
	defer conn.Close()

	b, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	if _, err := conn.Write(b); err != nil {
		return err
	}

	// Wait for the response and verify the operation went through.
	resp := response{}
	d := json.NewDecoder(conn)
	err = d.Decode(&resp)
	if err != nil {
		return err
	}

	if resp.Code != 0 {
		return fmt.Errorf(resp.Message)
	}
	return nil
}

// Write writes TCP msg to given server, for TCP join cluster
func (s *Service) write(server string, msg interface{}) error {
	conn, err := s.tn.Dial(server, connectionTimeout)
	if err != nil {
		return err
	}
	defer conn.Close()

	b, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	if _, err := conn.Write(b); err != nil {
		return err
	}

	// Wait for the response and verify the operation went through.
	resp := response{}
	d := json.NewDecoder(conn)
	err = d.Decode(&resp)
	if err != nil {
		return err
	}

	if resp.Code != 0 {
		return fmt.Errorf(resp.Message)
	}
	return nil
}

func (s *Service) handleConn(conn net.Conn) error {
	defer s.wg.Done()
	defer conn.Close()
	s.logger.Printf("received connection from %s", conn.RemoteAddr().String())

	// Only handles peers updates for now.
	msg := make(map[string][]byte)
	d := json.NewDecoder(conn)
	err := d.Decode(&msg)
	if err != nil {
		return err
	}

	t, ok := msg["type"]
	if !ok {
		return fmt.Errorf("no message type")
	}

	switch string(t) {
	case string(typPeer):
		s.handleSetPeer(msg, conn)
	case string(typCBucket):
		s.handleCreateBucket(msg, conn)
	case string(typCBucketNotExist):
		s.handleCreateBucketIfNotExist(msg, conn)
	case string(typRBucket):
		s.handleRemoveBucket(msg, conn)
	case string(typUpdate):
		s.handleUpdate(msg, conn)
	case string(typBatch):
		s.handleBatch(msg, conn)
	case string(typRkey):
		s.handleRemoveKey(msg, conn)

	case string(typSetSession):
		s.handleSetSession(msg, conn)
	case string(typDelSession):
		s.handleDelSession(msg, conn)

	case string(typJoin):
		s.handleJoin(msg, conn)
	case string(typRemove):
		s.handleRemove(msg, conn)
	default:
		return fmt.Errorf("unknown message type: %s", string(t))
	}
	return nil
}

func (s *Service) writeResponse(resp interface{}, conn net.Conn) {
	defer conn.Close()
	b, err := json.Marshal(resp)
	if err != nil {
		s.logger.Printf("marshal resp error: %s", err.Error())
		return
	}
	if _, err := conn.Write(b); err != nil {
		s.logger.Printf("write resp error: %s", err.Error())
		return
	}
}

func (s *Service) handleSetPeer(msg map[string][]byte, conn net.Conn) {
	raftAddr, rok := msg["raft"]
	apiAddr, aok := msg["api"]
	if !rok || !aok {
		resp := response{1, "need para"}
		s.writeResponse(resp, conn)
		return
	}

	// Update the peers.
	if err := s.store.UpdateAPIPeers(map[string]string{string(raftAddr): string(apiAddr)}); err != nil {
		resp := response{1, err.Error()}
		s.writeResponse(resp, conn)
		return
	}
	s.writeResponse(response{}, conn)
	return
}

func (s *Service) handleJoin(msg map[string][]byte, conn net.Conn) {
	addr, ok := msg["addr"]
	if !ok {
		resp := response{1, "need para"}
		s.writeResponse(resp, conn)
		return
	}

	// Join the cluster.
	if err := s.Join(string(addr)); err != nil {
		resp := response{1, err.Error()}
		s.writeResponse(resp, conn)
		return
	}
	s.writeResponse(response{}, conn)
	return
}

func (s *Service) handleRemove(msg map[string][]byte, conn net.Conn) {
	addr, ok := msg["addr"]
	if !ok {
		resp := response{1, "need para"}
		s.writeResponse(resp, conn)
		return
	}

	// Remove from the cluster.
	if err := s.store.Remove(string(addr)); err != nil {
		resp := response{1, err.Error()}
		s.writeResponse(resp, conn)
		return
	}
	s.writeResponse(response{}, conn)
}

func (s *Service) handleCreateBucket(msg map[string][]byte, conn net.Conn) {
	name, ok := msg["name"]
	if !ok {
		resp := response{1, "need para"}
		s.writeResponse(resp, conn)
		return
	}

	if err := s.store.CreateBucket(name); err != nil {
		resp := response{1, err.Error()}
		s.writeResponse(resp, conn)
		return
	}
	s.writeResponse(response{}, conn)
	return
}

func (s *Service) handleCreateBucketIfNotExist(msg map[string][]byte, conn net.Conn) {
	name, ok := msg["name"]
	if !ok {
		resp := response{1, "need para"}
		s.writeResponse(resp, conn)
		return
	}

	if err := s.store.CreateBucketIfNotExist(name); err != nil {
		resp := response{1, err.Error()}
		s.writeResponse(resp, conn)
		return
	}
	s.writeResponse(response{}, conn)
	return
}

func (s *Service) handleRemoveBucket(msg map[string][]byte, conn net.Conn) {
	name, ok := msg["name"]
	if !ok {
		resp := response{1, "need para"}
		s.writeResponse(resp, conn)
		return
	}

	if err := s.store.RemoveBucket(name); err != nil {
		resp := response{1, err.Error()}
		s.writeResponse(resp, conn)
		return
	}
	s.writeResponse(response{}, conn)
	return
}

func (s *Service) handleUpdate(msg map[string][]byte, conn net.Conn) {
	var bucket, key, value []byte
	var ok bool

	if bucket, ok = msg["bucket"]; !ok {
		resp := response{1, "need para"}
		s.writeResponse(resp, conn)
		return
	}

	if key, ok = msg["key"]; !ok {
		resp := response{1, "need para"}
		s.writeResponse(resp, conn)
		return
	}

	if value, ok = msg["value"]; !ok {
		resp := response{1, "need para"}
		s.writeResponse(resp, conn)
		return
	}

	if err := s.store.Update(bucket, key, value); err != nil {
		resp := response{1, err.Error()}
		s.logger.Printf("cluster handleUpdate error: %s\n", err.Error())
		s.writeResponse(resp, conn)
		return
	}
	s.writeResponse(response{}, conn)
	return
}

func (s *Service) handleBatch(msg map[string][]byte, conn net.Conn) {
	var b []byte
	var rows []model.Row
	var ok bool

	if b, ok = msg["rows"]; !ok {
		resp := response{1, "need para"}
		s.writeResponse(resp, conn)
		return
	}

	reader := bytes.NewReader(b)
	d := json.NewDecoder(reader)
	err := d.Decode(&rows)
	if err != nil {
		resp := response{1, err.Error()}
		s.writeResponse(resp, conn)
		return
	}

	if err := s.store.Batch(rows); err != nil {
		resp := response{1, err.Error()}
		s.writeResponse(resp, conn)
		return
	}
	s.writeResponse(response{}, conn)
	return
}

func (s *Service) handleRemoveKey(msg map[string][]byte, conn net.Conn) {
	var bucket, key []byte
	var ok bool

	if bucket, ok = msg["bucket"]; !ok {
		resp := response{1, "need para"}
		s.writeResponse(resp, conn)
		return
	}

	if key, ok = msg["key"]; !ok {
		resp := response{1, "need para"}
		s.writeResponse(resp, conn)
		return
	}

	// Remove from the cluster.
	if err := s.store.RemoveKey(bucket, key); err != nil {
		resp := response{1, err.Error()}
		s.writeResponse(resp, conn)
		return
	}
	s.writeResponse(response{}, conn)
	return
}

func (s *Service) handleSetSession(msg map[string][]byte, conn net.Conn) {
	var key, value []byte
	var ok bool

	if value, ok = msg["value"]; !ok {
		resp := response{1, "need para"}
		s.writeResponse(resp, conn)
		return
	}

	if key, ok = msg["key"]; !ok {
		resp := response{1, "need para"}
		s.writeResponse(resp, conn)
		return
	}

	// Remove from the cluster.
	if err := s.store.SetSession(string(key), string(value)); err != nil {
		resp := response{1, err.Error()}
		s.writeResponse(resp, conn)
		return
	}
	s.writeResponse(response{}, conn)
	return
}

func (s *Service) handleDelSession(msg map[string][]byte, conn net.Conn) {
	var key []byte
	var ok bool

	if key, ok = msg["key"]; !ok {
		resp := response{1, "need para"}
		s.writeResponse(resp, conn)
		return
	}

	// Remove from the cluster.
	if err := s.store.DelSession(string(key)); err != nil {
		resp := response{1, err.Error()}
		s.writeResponse(resp, conn)
		return
	}
	s.writeResponse(response{}, conn)
	return
}
