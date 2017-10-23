package store

import (
	"container/list"
	"sync"
	"sync/atomic"

	"github.com/lodastack/log"
	"github.com/lodastack/store/model"
)

// EvictCallback is used to get a callback when a cache entry is evicted.
type EvictCallback func(bucket []byte, key []byte, value []byte)

// Cache implements a non-thread safe fixed size LRU cache.
type Cache struct {
	mu        sync.RWMutex
	count     int
	evictList *list.List
	items     map[string]map[string]*list.Element
	onEvict   EvictCallback

	size    uint64
	maxSize uint64

	enable bool
	stats  *CacheStatistics
	logger *log.Logger
}

// entry is used to hold a value in the evictList.
type entry struct {
	bucket []byte
	key    []byte
	value  []byte
}

func (e *entry) Size() int {
	return len(e.bucket) + len(e.key) + len(e.value)
}

// Statistics gathered by the Cache.
const (
	statCacheMemoryBytes  = "memBytes" // level: Size of in-memory cache in bytes
	statCachePurgeCount   = "purgeCount"
	statAddCount          = "addCount"
	statGetCount          = "getCount"
	statHitCount          = "hitCount"
	statMissCount         = "missCount"
	statRemoveCount       = "removeCount"
	statRemoveBucketCount = "removeBucketCount"
	statRemoveOldestCount = "removeOldestCount"
	statKeysCount         = "keysCount"
)

// CacheStatistics hold statistics related to the cache.
type CacheStatistics struct {
	MemSizeBytes      int64
	PurgeCount        int64
	AddCount          int64
	GetCount          int64
	HitCount          int64
	MissCount         int64
	RemoveCount       int64
	RemoveBucketCount int64
	RemoveOldestCount int64
	KeysCount         int64
}

// NewCache constructs an LRU cache of the given size.
func NewCache(maxSize uint64, onEvict EvictCallback) *Cache {
	// user config need check maxSize
	// if maxSize <= 0 {
	// 	return nil, errors.New("Must provide a positive size")
	// }
	c := &Cache{
		count:     0,
		maxSize:   maxSize,
		items:     make(map[string]map[string]*list.Element),
		evictList: list.New(),
		onEvict:   onEvict,
		stats:     &CacheStatistics{},
		logger:    log.New("INFO", "cache", model.LogBackend),
	}
	return c
}

// Open cache
func (c *Cache) Open() {
	c.enable = true
}

// Statistics returns statistics for periodic monitoring.
func (c *Cache) Statistics(tags map[string]string) []model.Statistic {
	return []model.Statistic{{
		Name: "store_cache",
		Tags: tags,
		Values: map[string]interface{}{
			statCacheMemoryBytes:  c.size,
			statCachePurgeCount:   atomic.LoadInt64(&c.stats.PurgeCount),
			statAddCount:          atomic.LoadInt64(&c.stats.AddCount),
			statHitCount:          atomic.LoadInt64(&c.stats.HitCount),
			statMissCount:         atomic.LoadInt64(&c.stats.MissCount),
			statGetCount:          atomic.LoadInt64(&c.stats.GetCount),
			statRemoveCount:       atomic.LoadInt64(&c.stats.RemoveCount),
			statRemoveBucketCount: atomic.LoadInt64(&c.stats.RemoveBucketCount),
			statRemoveOldestCount: atomic.LoadInt64(&c.stats.RemoveOldestCount),
			statKeysCount:         c.Len(),
		},
	}}
}

// Purge is used to completely clear the cache.
func (c *Cache) Purge() {
	if !c.enable {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	c.items = make(map[string]map[string]*list.Element)
	c.count = 0
	c.size = 0
	c.evictList.Init()
	atomic.AddInt64(&c.stats.PurgeCount, 1)
}

// Add adds a value to the cache.  Returns true if an eviction occurred.
func (c *Cache) Add(bucketName []byte, key []byte, value []byte) bool {
	if !c.enable {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	// Check for existing bucket
	var bucket map[string]*list.Element
	var ok bool

	bucketKey := string(bucketName)

	if bucket, ok = c.items[bucketKey]; !ok {
		bucket = make(map[string]*list.Element)
		c.logger.Printf("cache new bucket %s", bucketKey)
	}

	// copy bytes
	dst := make([]byte, len(value))
	copy(dst, value)

	// Check for existing item
	if ent, ok := bucket[string(key)]; ok {
		c.evictList.MoveToFront(ent)
		ent.Value.(*entry).value = dst
		return false
	}

	// Add new item
	ent := &entry{bucketName, key, dst}
	entry := c.evictList.PushFront(ent)
	bucket[string(key)] = entry

	c.items[bucketKey] = bucket
	c.size += uint64(ent.Size())
	c.count++
	atomic.AddInt64(&c.stats.AddCount, 1)

	// Verify size not exceeded
	evict := c.maxSize > 0 && c.size > c.maxSize
	if evict {
		c.removeOldest()
	}
	return evict
}

// Get looks up a key's value from the cache.
func (c *Cache) Get(bucket, key []byte) (value []byte, ok bool) {
	if !c.enable {
		return nil, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	atomic.AddInt64(&c.stats.GetCount, 1)
	if b, ok := c.items[string(bucket)]; ok {
		if ent, ok := b[string(key)]; ok {
			c.evictList.MoveToFront(ent)
			c.logger.Debugf("Hit cache, key: %s", string(key))
			src := ent.Value.(*entry).value
			dst := make([]byte, len(src))
			copy(dst, src)
			atomic.AddInt64(&c.stats.HitCount, 1)
			return dst, true
		}
	}
	atomic.AddInt64(&c.stats.MissCount, 1)
	return
}

// Contains checks if a key is in the cache, without updating the recent-ness
// or deleting it for being stale.
func (c *Cache) Contains(bucket, key []byte) (ok bool) {
	if !c.enable {
		return false
	}
	c.mu.RLock()
	defer c.mu.RUnlock()

	if b, bucketok := c.items[string(bucket)]; bucketok {
		_, ok = b[string(key)]
		return ok
	}
	return
}

// Peek returns the key value (or undefined if not found) without updating
// the "recently used"-ness of the key.
func (c *Cache) Peek(bucket, key []byte) (value []byte, ok bool) {
	if !c.enable {
		return
	}
	c.mu.RLock()
	defer c.mu.RUnlock()

	if b, ok := c.items[string(bucket)]; ok {
		if ent, ok := b[string(key)]; ok {
			return ent.Value.(*entry).value, true
		}
	}
	return
}

// RemoveBucket remove Bucket removes the provided bucket from the cache, returning if the
// bucket was contained.
func (c *Cache) RemoveBucket(bucket []byte) bool {
	if !c.enable {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	if b, ok := c.items[string(bucket)]; ok {
		for _, ent := range b {
			c.removeElement(ent)
		}
		delete(c.items, string(bucket))
		atomic.AddInt64(&c.stats.RemoveBucketCount, 1)
		return true
	}
	return false
}

// Remove removes the provided key from the cache, returning if the
// key was contained.
func (c *Cache) Remove(bucket, key []byte) bool {
	if !c.enable {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	if b, ok := c.items[string(bucket)]; ok {
		if ent, ok := b[string(key)]; ok {
			c.removeElement(ent)
			atomic.AddInt64(&c.stats.RemoveCount, 1)
			return true
		}
	}
	return false
}

// RemoveOldest removes the oldest item from the cache.
func (c *Cache) RemoveOldest() ([]byte, []byte, []byte, bool) {
	if !c.enable {
		return nil, nil, nil, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	ent := c.evictList.Back()
	if ent != nil {
		c.removeElement(ent)
		kv := ent.Value.(*entry)
		atomic.AddInt64(&c.stats.RemoveOldestCount, 1)
		return kv.bucket, kv.key, kv.value, true
	}
	return nil, nil, nil, false
}

// GetOldest returns the oldest entry.
func (c *Cache) GetOldest() ([]byte, []byte, []byte, bool) {
	if !c.enable {
		return nil, nil, nil, false
	}
	c.mu.RLock()
	defer c.mu.RUnlock()

	ent := c.evictList.Back()
	if ent != nil {
		kv := ent.Value.(*entry)
		return kv.bucket, kv.key, kv.value, true
	}
	return nil, nil, nil, false
}

// Keys returns a slice of the keys in the cache, from oldest to newest.
func (c *Cache) Keys() []string {
	if !c.enable {
		return nil
	}
	c.mu.RLock()
	defer c.mu.RUnlock()

	var keys []string
	i := 0
	for ent := c.evictList.Back(); ent != nil; ent = ent.Prev() {
		keys[i] = string(ent.Value.(*entry).bucket) + "-" + string(ent.Value.(*entry).key)
		i++
	}
	return keys
}

// Len returns the number of items in the cache.
func (c *Cache) Len() int {
	if !c.enable {
		return 0
	}
	return c.evictList.Len()
}

// removeOldest removes the oldest item from the cache.
func (c *Cache) removeOldest() {
	ent := c.evictList.Back()
	if ent != nil {
		atomic.AddInt64(&c.stats.RemoveOldestCount, 1)
		c.removeElement(ent)
	}
}

// removeElement is used to remove a given list element from the cache.
func (c *Cache) removeElement(e *list.Element) {
	c.evictList.Remove(e)
	kv := e.Value.(*entry)
	if bucket, ok := c.items[string(kv.bucket)]; ok {
		delete(bucket, string(kv.key))
		c.size -= uint64(kv.Size())
		c.count--
		if c.onEvict != nil {
			c.onEvict(kv.bucket, kv.key, kv.value)
		}
	}
}
