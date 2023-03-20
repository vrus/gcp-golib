/*
 * Copyright (c) 2020. Victor Ruscitto (vrus@vrcyber.com). All rights reserved.
 */

package storage

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/gomodule/redigo/redis"
)

type MemStore struct {
	pool *redis.Pool
}

// NewMemstore
func NewMemstore(host string, port string, password string) (*MemStore, error) {
	if len(host) == 0 {
		return nil, errors.New("Memstore host cannot be blank")
	}
	if len(port) == 0 {
		return nil, errors.New("Memstore port cannot be blank")
	}

	redisAddr := fmt.Sprintf("%s:%s", host, port)

	redisPool := &redis.Pool{
		MaxIdle:     3,
		IdleTimeout: 240 * time.Second,
		// Dial or DialContext must be set. When both are set, DialContext takes precedence over Dial.
		Dial: func() (redis.Conn, error) {
			conn, err := redis.Dial("tcp", redisAddr)
			if err != nil {
				return nil, err
			}
			// If a password is supplied, we need to submit an AUTH request or Redis will not allow subsequent calls
			if len(password) > 0 {
				if _, err := conn.Do("AUTH", password); err != nil {
					return nil, err
				}
			}

			return conn, nil
		},
	}

	// When using a connection pool, you only get connection errors while trying to send commands.
	// Try to PING so we can fail-fast in the case of invalid address.
	conn := redisPool.Get()
	defer conn.Close()

	if _, err := conn.Do("PING"); err != nil {
		return nil, err
	}

	return &MemStore{
		pool: redisPool,
	}, nil
}

// IncrementAndExpire will attempt to increment a key if it doesnt exceed MaxValue
func (m *MemStore) IncrementAndExpire(key string, maxValue int, expiry float64) (int, bool) {
	conn := m.pool.Get()
	defer conn.Close()

	tokens, err := redis.Int(conn.Do("GET", key))
	//log.Printf("Tokens %v", tokens)

	if err != nil && err != redis.ErrNil {
		log.Printf("failed to retrieve key %v. %v", key, err)
		return 0, false
	}

	if maxValue > 0 && tokens >= maxValue {
		return tokens, false
	}

	conn.Do("MULTI")
	tokens, err = redis.Int(conn.Do("INCR", key))
	conn.Do("EXPIRE", key, expiry)
	conn.Do("EXEC")

	return tokens, true
}

// KeyExists checks if a key exists
func (m *MemStore) KeyExists(key string) bool {
	conn := m.pool.Get()
	defer conn.Close()

	val, err := redis.Int(conn.Do("EXISTS", key))

	if err != nil && err != redis.ErrNil {
		log.Printf("failed to check key exists %v. %v", key, err)
		return false
	}

	if val == 0 {
		return false
	} else {
		return true
	}
}

// GetIntKey
func (m *MemStore) GetIntKey(key string) int {
	conn := m.pool.Get()
	defer conn.Close()

	val, err := redis.Int(conn.Do("GET", key))

	if err != nil && err != redis.ErrNil {
		log.Printf("failed to increment key %v. %v", key, err)
		return -1
	}

	return val
}

// GetKey returns string value of a key
func (m *MemStore) GetKey(key string, dest interface{}) error {
	conn := m.pool.Get()
	defer conn.Close()

	val, err := redis.Bytes(conn.Do("GET", key))

	if err != nil && err != redis.ErrNil {
		log.Printf("failed to get key %v. %v", key, err)
		return err
	}

	return json.Unmarshal(val, dest)
}

// SaveKey: if you pass in expiry > 0 it will expire the key. expiry is in seconds
func (m *MemStore) SaveKey(key string, val interface{}, expiry int) bool {
	var err error
	var status string

	conn := m.pool.Get()
	defer conn.Close()

	data, err := json.Marshal(val)
	if err != nil {
		return false
	}

	if expiry > 0 {
		status, err = redis.String(conn.Do("SETEX", key, expiry, data))
	} else {
		status, err = redis.String(conn.Do("SET", key, data))
	}

	if err != nil {
		return false
	} else {
		if status != "OK" {
			return false
		}
	}

	return true
}

// DeleteKey removes a key from MemStore
func (m *MemStore) DeleteKey(key string) bool {
	conn := m.pool.Get()
	defer conn.Close()

	if count, _ := redis.Int(conn.Do("DEL", key)); count == 0 {
		return false
	}

	return true
}

// IncrementKey will attempt to increment a key
func (m *MemStore) IncrementKey(key string) int {
	conn := m.pool.Get()
	defer conn.Close()

	value, err := redis.Int(conn.Do("INCR", key))
	//log.Printf("Value %v", value)

	if err != nil && err != redis.ErrNil {
		log.Printf("failed to increment key %v. %v", key, err)
		return -1
	}

	return value
}

// GetIntSet
func (m *MemStore) GetIntSet(key string) ([]int, bool) {
	conn := m.pool.Get()
	defer conn.Close()

	if vals, err := redis.Ints(conn.Do("SMEMBERS", key)); err != nil {
		return nil, false
	} else {
		return vals, true
	}
}

// AddIntToSet
func (m *MemStore) AddIntToSet(key string, val int) bool {
	conn := m.pool.Get()
	defer conn.Close()

	_, err := redis.Int(conn.Do("SADD", key, val))
	if err != nil {
		return false
	} else {
		return true
	}
}

func (m *MemStore) AddStringToSet(key string, val string) bool {
	conn := m.pool.Get()
	defer conn.Close()

	if affected, err := redis.Int(conn.Do("SADD", key, val)); err != nil || affected == 0 {
		return false
	} else {
		return true
	}
}

func (m *MemStore) IsStringInSet(key string, val string) bool {
	conn := m.pool.Get()
	defer conn.Close()

	if affected, err := redis.Int(conn.Do("SISMEMBER", key, val)); err != nil || affected == 0 {
		return false
	} else {
		return true
	}
}
