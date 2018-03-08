package store

import (
	"testing"
)

func Test_NewSession(t *testing.T) {
	s := NewSession()
	if s == nil {
		t.Fatalf("new session failed: %v", s)
	}
}

func Test_SetAndGet(t *testing.T) {
	s := NewSession()
	if s == nil {
		t.Fatalf("new session failed: %v", s)
	}
	s.Set("token", "username")
	res := s.Get("token")
	username := res.(string)
	if username != "username" {
		t.Fatalf("get failed failed: %s - %s", username, "username")
	}
}

func Test_Clean(t *testing.T) {
	s := NewSession()
	if s == nil {
		t.Fatalf("new session failed: %v", s)
	}
	s.Set("token1", "username")
	s.Set("token2", "username")
	s.Set("token3", "username")
	s.Set("token4", "username")

	res := s.Get("token1")
	_, ok1 := res.(string)
	res = s.Get("token2")
	_, ok2 := res.(string)
	res = s.Get("token3")
	_, ok3 := res.(string)
	if ok1 && ok2 && ok3 {
		t.Fatalf("should not get all token: %v %v %v", ok1, ok2, ok3)
	}

	res = s.Get("token4")
	username, ok := res.(string)
	if !ok {
		t.Fatalf("should get this token: %s", username)
	}
	res = s.Get("token4")
	username = res.(string)
	if username != "username" {
		t.Fatalf("get failed failed: %s - %s", username, "username")
	}
}

func Test_Delete(t *testing.T) {
	s := NewSession()
	if s == nil {
		t.Fatalf("new session failed: %v", s)
	}
	s.Set("token", "username")
	s.Delete("token")
	res := s.Get("token")
	if res != nil {
		t.Fatalf("get failed failed: %v", res)
	}
}
