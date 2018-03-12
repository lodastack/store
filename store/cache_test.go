package store

import (
	"testing"

	"github.com/lodastack/store/log"
)

var bucket1 = []byte("test-bucket")
var key1 = []byte("test-key")
var value1 = []byte("test-value123")

var bucket2 = []byte("test-bucket2")
var key2 = []byte("test-key2")
var value2 = []byte("test-value456")

var bucket3 = []byte("test-bucket3")
var key3 = []byte("test-key3")
var value3 = []byte("test-value789")

func TestCache(t *testing.T) {
	c := NewCache(10, nil, log.New())
	c.Open()

	if c.maxSize != 10 {
		t.Fatalf("new cache err, expect maxSize: %d - %d", 10, c.maxSize)
	}
}

func Test_Add_Get(t *testing.T) {
	c := NewCache(1024, nil, log.New())
	c.Open()

	c.Add(bucket1, key1, value1)
	c.Add(bucket2, key2, value2)

	v, exist := c.Get(bucket1, key1)
	if !exist || string(v) != string(value1) {
		t.Fatalf("Add Key err: %s - %s ", string(v), string(value1))
	}

	v, exist = c.Get(bucket2, key2)
	if !exist || string(v) != string(value2) {
		t.Fatalf("Add Key err: %s - %s ", string(v), string(value2))
	}
}

func Test_LRU_FullMem(t *testing.T) {
	c := NewCache(40, nil, log.New())
	c.Open()

	c.Add(bucket1, key1, value1)
	c.Add(bucket2, key2, value2)
	c.Add(bucket3, key3, value3)

	v, exist := c.Get(bucket1, key1)
	if exist || string(v) == string(value1) {
		t.Fatalf("key should be evicted")
	}

	v, exist = c.Get(bucket2, key2)
	if exist || string(v) == string(value2) {
		t.Fatalf("key should be evicted")
	}

	v, exist = c.Get(bucket3, key3)
	if !exist || string(v) != string(value3) {
		t.Fatalf("Get Key err: %s - %s ", string(v), string(value3))
	}
}

func Test_LRU(t *testing.T) {
	c := NewCache(70, nil, log.New())
	c.Open()

	c.Add(bucket1, key1, value1)
	c.Add(bucket2, key2, value2)
	c.Add(bucket3, key3, value3)

	c.Get(bucket2, key2)

	c.Add(bucket1, key2, value2)

	c.Get(bucket2, key2)

	c.Add(bucket2, key1, value1)

	v, exist := c.Get(bucket1, key1)
	if exist || string(v) == string(value1) {
		t.Fatalf("key should be evicted")
	}

	v, exist = c.Get(bucket3, key3)
	if exist || string(v) == string(value3) {
		t.Fatalf("key should be evicted")
	}

	v, exist = c.Get(bucket2, key2)
	if !exist || string(v) != string(value2) {
		t.Fatalf("Get Key err: %s - %s ", string(v), string(value2))
	}
}

func Test_RemoveBucket(t *testing.T) {
	c := NewCache(1024, nil, log.New())
	c.Open()

	c.Add(bucket1, key1, value1)
	c.Add(bucket2, key2, value2)
	c.Add(bucket3, key3, value3)

	c.RemoveBucket(bucket1)

	v, exist := c.Get(bucket1, key1)
	if exist || string(v) == string(value1) {
		t.Fatalf("key should be removed")
	}

	v, exist = c.Get(bucket2, key2)
	if !exist || string(v) != string(value2) {
		t.Fatalf("Get Key err: %s - %s ", string(v), string(value2))
	}

	v, exist = c.Get(bucket3, key3)
	if !exist || string(v) != string(value3) {
		t.Fatalf("Get Key err: %s - %s ", string(v), string(value3))
	}
}

func Test_Remove(t *testing.T) {
	c := NewCache(1024, nil, log.New())
	c.Open()

	c.Add(bucket1, key1, value1)
	c.Add(bucket1, key2, value2)
	c.Add(bucket2, key2, value2)
	c.Add(bucket3, key3, value3)

	c.Remove(bucket1, key1)

	v, exist := c.Get(bucket1, key1)
	if exist || string(v) == string(value1) {
		t.Fatalf("key should be removed")
	}

	v, exist = c.Get(bucket1, key2)
	if !exist || string(v) != string(value2) {
		t.Fatalf("Get Key err: %s - %s ", string(v), string(value2))
	}

	v, exist = c.Get(bucket2, key2)
	if !exist || string(v) != string(value2) {
		t.Fatalf("Get Key err: %s - %s ", string(v), string(value2))
	}

	v, exist = c.Get(bucket3, key3)
	if !exist || string(v) != string(value3) {
		t.Fatalf("Get Key err: %s - %s ", string(v), string(value3))
	}
}

func Test_Purge(t *testing.T) {
	c := NewCache(40, nil, log.New())
	c.Open()

	c.Add(bucket1, key1, value1)
	c.Add(bucket2, key2, value2)
	c.Add(bucket3, key3, value3)

	c.Purge()

	v, exist := c.Get(bucket1, key1)
	if exist || string(v) == string(value1) {
		t.Fatalf("key should be evicted")
	}

	v, exist = c.Get(bucket2, key2)
	if exist || string(v) == string(value2) {
		t.Fatalf("key should be evicted")
	}

	v, exist = c.Get(bucket3, key3)
	if exist || string(v) == string(value3) {
		t.Fatalf("key should be evicted")
	}
}
