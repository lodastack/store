package store

import (
	"sync"
)

const maximumOnline = 3

// Session interface
// key is user token, value is user id.
type Session interface {
	// Get returns the session value associated to the given key.
	Get(key interface{}) interface{}
	// Set sets the session value associated to the given key.
	Set(key interface{}, val interface{})
	// Delete removes the session value associated to the given key.
	Delete(key interface{})
}

// NewSession returns a new session
func NewSession() *LodaSession {
	m := make(map[interface{}]interface{})
	return &LodaSession{SessionMap: m}
}

// LodaSession struct
type LodaSession struct {
	//SessionMap store all client token
	SessionMap map[interface{}]interface{}
	// Mux locks SessionMap
	Mux sync.Mutex
}

// Get returns the session value associated to the given key.
func (ls *LodaSession) Get(i interface{}) interface{} {
	ls.Mux.Lock()
	defer ls.Mux.Unlock()
	return ls.SessionMap[i]
}

// Set sets the session value associated to the given key.
func (ls *LodaSession) Set(k, v interface{}) {
	ls.clean(v)
	ls.Mux.Lock()
	ls.SessionMap[k] = v
	ls.Mux.Unlock()
}

// Delete removes the session value associated to the given key.
func (ls *LodaSession) Delete(k interface{}) {
	ls.Mux.Lock()
	defer ls.Mux.Unlock()
	ls.SessionMap[k] = nil
	delete(ls.SessionMap, k)
}

// Clean dirty data in session
func (ls *LodaSession) clean(uid interface{}) {
	uidStr, ok := uid.(string)
	if !ok {
		return
	}
	ls.Mux.Lock()
	defer ls.Mux.Unlock()

	online := 1
	for k, v := range ls.SessionMap {
		if valueStr, ok := v.(string); ok {
			if valueStr == uidStr {
				online++
				if online > maximumOnline {
					ls.SessionMap[k] = nil
					delete(ls.SessionMap, k)
				}
			}
		}
	}
}
