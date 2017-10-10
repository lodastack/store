package store

import (
	"io/ioutil"
	"net"
	"os"
	"sort"
	"testing"
	"time"

	"github.com/lodastack/log"
)

const (
	bucket = "test-bucket"
	key    = "test-key"
	value  = "mdzz123"
)

func Test_IsLeader(t *testing.T) {
	s := mustNewStore()
	defer os.RemoveAll(s.Path())

	if err := s.Open(true); err != nil {
		t.Fatalf("failed to open single-node store: %s", err.Error())
	}
	defer s.Close(true)
	s.WaitForLeader(10 * time.Second)

	if !s.IsLeader() {
		t.Fatalf("single node is not leader!")
	}
}

func Test_OpenCloseStore(t *testing.T) {
	s := mustNewStore()
	defer os.RemoveAll(s.Path())

	if err := s.Open(true); err != nil {
		t.Fatalf("failed to open single-node store: %s", err.Error())
	}

	if err := s.Close(true); err != nil {
		t.Fatalf("failed to close single-node store: %s", err.Error())
	}
}

func Test_SingleNode_CreateRemoveBucket(t *testing.T) {
	s := mustNewStore()
	defer os.RemoveAll(s.Path())

	if err := s.Open(true); err != nil {
		t.Fatalf("failed to open single-node store: %s", err.Error())
	}
	defer s.Close(true)
	s.WaitForLeader(10 * time.Second)

	if err := s.CreateBucket([]byte(bucket)); err != nil {
		t.Fatalf("failed to create bucket: %s", err.Error())
	}

	if err := s.RemoveBucket([]byte(bucket)); err != nil {
		t.Fatalf("failed to remove bucket: %s", err.Error())
	}
}

func Test_SingleNode_SetGetKey(t *testing.T) {
	s := mustNewStore()
	defer os.RemoveAll(s.Path())

	if err := s.Open(true); err != nil {
		t.Fatalf("failed to open single-node store: %s", err.Error())
	}
	defer s.Close(true)
	s.WaitForLeader(10 * time.Second)

	if err := s.CreateBucket([]byte(bucket)); err != nil {
		t.Fatalf("failed to create bucket: %s", err.Error())
	}

	if err := s.Update([]byte(bucket), []byte(key), []byte(value)); err != nil {
		t.Fatalf("failed to update key: %s", err.Error())
	}

	var v []byte
	var err error
	if v, err = s.View([]byte(bucket), []byte(key)); err != nil {
		t.Fatalf("failed to get key: %s", err.Error())
	}

	if string(v) != value {
		t.Fatalf("funexpected results for get: %s - %s ", string(v), value)
	}
}

func Test_SingleNode_DeleteKey(t *testing.T) {
	s := mustNewStore()
	defer os.RemoveAll(s.Path())
	if err := s.Open(true); err != nil {
		t.Fatalf("failed to open single-node store: %s", err.Error())
	}
	defer s.Close(true)
	s.WaitForLeader(10 * time.Second)
	if err := s.CreateBucket([]byte(bucket)); err != nil {
		t.Fatalf("failed to create bucket: %s", err.Error())
	}
	if err := s.Update([]byte(bucket), []byte(key), []byte(value)); err != nil {
		t.Fatalf("failed to update key: %s", err.Error())
	}

	var v []byte
	var err error
	if v, err = s.View([]byte(bucket), []byte(key)); err != nil {
		t.Fatalf("failed to get key: %s", err.Error())
	}
	if string(v) != value {
		t.Fatalf("funexpected results for get: %s - %s ", string(v), value)
	}

	if err := s.RemoveKey([]byte(bucket), []byte(key)); err != nil {
		t.Fatalf("failed to delete key: %s", err.Error())
	}
	if v, err = s.View([]byte(bucket), []byte(key)); err != nil || len(v) != 0 {
		t.Fatalf("get the removed key success, the output: %s, error: %v", v, err)
	}
}

func Test_SingleNode_GetAfterAnotherBucketSetKey(t *testing.T) {
	s := mustNewStore()
	defer os.RemoveAll(s.Path())

	if err := s.Open(true); err != nil {
		t.Fatalf("failed to open single-node store: %s", err.Error())
	}
	defer s.Close(true)
	s.WaitForLeader(10 * time.Second)

	if err := s.CreateBucket([]byte(bucket)); err != nil {
		t.Fatalf("failed to create bucket: %s", err.Error())
	}

	if err := s.Update([]byte(bucket), []byte(key), []byte(value)); err != nil {
		t.Fatalf("failed to update key: %s", err.Error())
	}

	var v []byte
	var err error
	// Get at first
	if v, err = s.View([]byte(bucket), []byte(key)); err != nil {
		t.Fatalf("failed to get key: %s", err.Error())
	}

	if string(v) != value {
		t.Fatalf("funexpected results for get: %s - %s ", string(v), value)
	}

	// Another Bucket
	if err := s.CreateBucket([]byte("another")); err != nil {
		t.Fatalf("failed to create bucket: %s", err.Error())
	}

	if err := s.Update([]byte("another"), []byte(key), []byte(value)); err != nil {
		t.Fatalf("failed to update key: %s", err.Error())
	}

	// Test Result
	if v, err = s.View([]byte(bucket), []byte(key)); err != nil {
		t.Fatalf("failed to get key: %s", err.Error())
	}

	if string(v) != value {
		t.Fatalf("funexpected results for get: %s - %s ", string(v), value)
	}
}

func Test_MultiNode_SetGetKey(t *testing.T) {
	s0 := mustNewStore()
	defer os.RemoveAll(s0.Path())
	if err := s0.Open(true); err != nil {
		t.Fatalf("failed to open node for multi-node test: %s", err.Error())
	}
	defer s0.Close(true)
	s0.WaitForLeader(10 * time.Second)

	s1 := mustNewStore()
	defer os.RemoveAll(s1.Path())
	if err := s1.Open(false); err != nil {
		t.Fatalf("failed to open node for multi-node test: %s", err.Error())
	}
	defer s1.Close(true)

	// Join the second node to the first.
	if err := s0.Join(s1.Addr()); err != nil {
		t.Fatalf("failed to join to node at %s: %s", s0.Addr(), err.Error())
	}

	if err := s0.CreateBucket([]byte(bucket)); err != nil {
		t.Fatalf("failed to create bucket: %s", err.Error())
	}

	if err := s0.Update([]byte(bucket), []byte(key), []byte(value)); err != nil {
		t.Fatalf("failed to update key: %s", err.Error())
	}

	time.Sleep(150 * time.Millisecond)

	var v []byte
	var err error
	if v, err = s1.View([]byte(bucket), []byte(key)); err != nil {
		t.Fatalf("failed to get key: %s", err.Error())
	}

	if string(v) != value {
		t.Fatalf("funexpected results for get: %s - %s ", string(v), value)
	}
}

func Test_MultiNode_RemoveKey(t *testing.T) {
	s0 := mustNewStore()
	defer os.RemoveAll(s0.Path())
	if err := s0.Open(true); err != nil {
		t.Fatalf("failed to open node for multi-node test: %s", err.Error())
	}
	defer s0.Close(true)
	s0.WaitForLeader(10 * time.Second)
	s1 := mustNewStore()
	defer os.RemoveAll(s1.Path())
	if err := s1.Open(false); err != nil {
		t.Fatalf("failed to open node for multi-node test: %s", err.Error())
	}
	defer s1.Close(true)

	// Join the second node to the first.
	if err := s0.Join(s1.Addr()); err != nil {
		t.Fatalf("failed to join to node at %s: %s", s0.Addr(), err.Error())
	}
	if err := s0.CreateBucket([]byte(bucket)); err != nil {
		t.Fatalf("failed to create bucket: %s", err.Error())
	}
	if err := s0.Update([]byte(bucket), []byte(key), []byte(value)); err != nil {
		t.Fatalf("failed to update key: %s", err.Error())
	}

	time.Sleep(150 * time.Millisecond)

	var v []byte
	var err error
	if v, err = s1.View([]byte(bucket), []byte(key)); err != nil {
		t.Fatalf("failed to get key: %s", err.Error())
	}
	if string(v) != value {
		t.Fatalf("funexpected results for get: %s - %s ", string(v), value)
	}

	if err := s0.RemoveKey([]byte(bucket), []byte(key)); err != nil {
		t.Fatalf("failed to remove key: %s", err.Error())
	}
	time.Sleep(150 * time.Millisecond)
	if v, err = s1.View([]byte(bucket), []byte(key)); err != nil || len(v) != 0 {
		t.Fatalf("get the removed  key success, output: %s, error: %v", v, err)
	}
}

func Test_MultiNode_SetGetRemoveSession(t *testing.T) {
	s0 := mustNewStore()
	defer os.RemoveAll(s0.Path())
	if err := s0.Open(true); err != nil {
		t.Fatalf("failed to open node for multi-node test: %s", err.Error())
	}
	defer s0.Close(true)
	s0.WaitForLeader(10 * time.Second)

	s1 := mustNewStore()
	defer os.RemoveAll(s1.Path())
	if err := s1.Open(false); err != nil {
		t.Fatalf("failed to open node for multi-node test: %s", err.Error())
	}
	defer s1.Close(true)

	// Join the second node to the first.
	if err := s0.Join(s1.Addr()); err != nil {
		t.Fatalf("failed to join to node at %s: %s", s0.Addr(), err.Error())
	}

	if err := s0.SetSession("u1", "t1"); err != nil {
		t.Fatalf("failed to set session: %s", err.Error())
	}

	time.Sleep(150 * time.Millisecond)

	var v interface{}
	v = s1.GetSession("u1")
	vStr, ok := v.(string)
	if !ok || vStr != "t1" {
		t.Fatalf("funexpected results for get: %s - %s ", v.(string), "t1")
	}

	// remove
	if err := s0.DelSession("u1"); err != nil {
		t.Fatalf("failed to remove session: %s", err.Error())
	}

	time.Sleep(150 * time.Millisecond)
	v = s1.GetSession("u1")
	if v != nil {
		t.Fatalf("funexpected results for get: %s - %s ", v.(string), nil)
	}

}

func Test_MultiNode_JoinRemove(t *testing.T) {
	s0 := mustNewStore()
	defer os.RemoveAll(s0.Path())
	if err := s0.Open(true); err != nil {
		t.Fatalf("failed to open node for multi-node test: %s", err.Error())
	}
	defer s0.Close(true)
	s0.WaitForLeader(10 * time.Second)

	s1 := mustNewStore()
	defer os.RemoveAll(s1.Path())
	if err := s1.Open(false); err != nil {
		t.Fatalf("failed to open node for multi-node test: %s", err.Error())
	}
	defer s1.Close(true)

	// Get sorted list of cluster nodes.
	storeNodes := []string{s0.Addr(), s1.Addr()}
	sort.StringSlice(storeNodes).Sort()

	// Join the second node to the first.
	if err := s0.Join(s1.Addr()); err != nil {
		t.Fatalf("failed to join to node at %s: %s", s0.Addr(), err.Error())
	}

	nodes, err := s0.Nodes()
	if err != nil {
		t.Fatalf("failed to get nodes: %s", err.Error())
	}
	sort.StringSlice(nodes).Sort()

	if len(nodes) != len(storeNodes) {
		t.Fatalf("size of cluster is not correct")
	}
	if storeNodes[0] != nodes[0] && storeNodes[1] != nodes[1] {
		t.Fatalf("cluster does not have correct nodes")
	}

	// Remove a node.
	if err := s0.Remove(s1.Addr()); err != nil {
		t.Fatalf("failed to remove %s from cluster: %s", s1.Addr(), err.Error())
	}

	nodes, err = s0.Nodes()
	if err != nil {
		t.Fatalf("failed to get nodes post remove: %s", err.Error())
	}
	if len(nodes) != 1 {
		t.Fatalf("size of cluster is not correct post remove")
	}
	if s0.Addr() != nodes[0] {
		t.Fatalf("cluster does not have correct nodes post remove")
	}
}

func mustNewStore() *Store {
	path := mustTempDir()

	s := New(path, mustMockTransport())
	if s == nil {
		panic("failed to create new store")
	}
	s.logger = log.GetLogger()
	s.cache.logger = s.logger
	return s
}

func mustTempDir() string {
	var err error
	path, err := ioutil.TempDir("", "registry-test-")
	if err != nil {
		panic("failed to create temp dir")
	}
	return path
}

type mockTransport struct {
	ln net.Listener
}

func mustMockTransport() Transport {
	ln, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		panic("failed to create new transport" + err.Error())
	}
	return &mockTransport{ln}
}

func (m *mockTransport) Dial(addr string, timeout time.Duration) (net.Conn, error) {
	return net.DialTimeout("tcp", addr, timeout)
}

func (m *mockTransport) Accept() (net.Conn, error) { return m.ln.Accept() }

func (m *mockTransport) Close() error { return m.ln.Close() }

func (m *mockTransport) Addr() net.Addr { return m.ln.Addr() }
