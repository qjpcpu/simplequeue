package qio

import (
	"encoding/json"
	"github.com/garyburd/redigo/redis"
)

var (
	popScript = redis.NewScript(1, `
local vals=redis.call("ZRANGEBYSCORE", KEYS[1],0,ARGV[1],"LIMIT",0,1)
if table.getn(vals) > 0 then
  redis.call("ZREM",KEYS[1],vals[1])
  return vals[1]
end
return nil
`)
)

// clock API
func PopDelay(conn redis.Conn, topic string, score uint64) (interface{}, error) {
	return popScript.Do(conn, topic, score)
}

func AddDelay(conn redis.Conn, topic string, payload interface{}, score uint64) error {
	_, err := conn.Do("ZADD", topic, score, payload)
	return err
}

func PopDelayString(conn redis.Conn, topic string, score uint64) (string, error) {
	return redis.String(PopDelay(conn, topic, score))
}

func AddDelayString(conn redis.Conn, topic string, payload string, score uint64) error {
	return AddDelay(conn, topic, payload, score)
}

func PopDelayJSON(conn redis.Conn, topic string, score uint64, obj interface{}) error {
	data, err := redis.Bytes(PopDelay(conn, topic, score))
	if err != nil {
		return err
	}
	return json.Unmarshal(data, obj)
}

func AddDelayJSON(conn redis.Conn, topic string, obj interface{}, score uint64) error {
	data, err := json.Marshal(obj)
	if err != nil {
		return err
	}
	return AddDelay(conn, topic, data, score)
}

// normal API
func Pop(conn redis.Conn, topic string) (interface{}, error) {
	return conn.Do("LPOP", topic)
}

func Add(conn redis.Conn, topic string, payload interface{}) error {
	_, err := conn.Do("RPUSH", topic, payload)
	return err
}

func PopString(conn redis.Conn, topic string) (string, error) {
	return redis.String(Pop(conn, topic))
}

func AddString(conn redis.Conn, topic string, payload string) error {
	return Add(conn, topic, payload)
}

func PopJSON(conn redis.Conn, topic string, obj interface{}) error {
	data, err := redis.Bytes(Pop(conn, topic))
	if err != nil {
		return err
	}
	return json.Unmarshal(data, obj)
}

func AddJSON(conn redis.Conn, topic string, obj interface{}) error {
	data, err := json.Marshal(obj)
	if err != nil {
		return err
	}
	return Add(conn, topic, data)
}
