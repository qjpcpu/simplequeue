package qio

import (
	"github.com/garyburd/redigo/redis"
	"testing"
)

func TestIO(t *testing.T) {
	conn, _ := redis.Dial("tcp", ":6379")
	msg := "first message"
	Add(conn, "qio", msg, 0)
	str, err := PopString(conn, "qio", 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(str)
	t.Log(PopString(conn, "qio", 0))
}

func TestJSON(t *testing.T) {
	conn, _ := redis.Dial("tcp", ":6379")
	type obj struct {
		Name string `json:"name"`
		Age  int
	}
	AddJSON(conn, "qjs", obj{Name: "jack", Age: 11}, 0)
	var o obj
	if err := PopJSON(conn, "qjs", 0, &o); err != nil {
		t.Fatal(err)
	}
	if o.Name != "jack" || o.Age != 11 {
		t.Fatal(o)
	}
	AddJSON(conn, "qjs", obj{Name: "jack", Age: 119}, 0)
	t.Log(PopString(conn, "qjs", 0))
}
