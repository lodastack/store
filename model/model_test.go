package model_test

import (
	"testing"

	"github.com/lodastack/store/model"
)

func TestContainString(t *testing.T) {
	examples := []struct {
		Base   []string
		Arg    string
		Result bool
	}{
		{
			Base:   nil,
			Arg:    "",
			Result: false,
		},
		{
			Base:   nil,
			Arg:    "foo",
			Result: false,
		},
		{
			Base:   []string{"foo", "bar"},
			Arg:    "",
			Result: false,
		},
		{
			Base:   []string{"foo", "foo"},
			Arg:    "foo",
			Result: true,
		},
		{
			Base:   []string{"foo", "bar"},
			Arg:    "bar",
			Result: true,
		},
		{
			Base:   []string{"foo", "bar"},
			Arg:    "fo",
			Result: false,
		},
	}

	for i, example := range examples {
		_, result := model.ContainString(example.Base, example.Arg)
		if result != example.Result {
			t.Errorf("[Example %d] got %#v, expected %#v", i, result, example.Result)
		}
	}
}
