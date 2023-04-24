package redis

import (
	"testing"
	"time"
)

var config = Config{
	Host:      "",
	Password:  "",
	Database:  0,
	IsCluster: true,
	Enable:    true,
}

func TestGSet(t *testing.T) {
	redisHandler := NewRedisHandler(&config)
	redisHandler.Set("test", "a", time.Second)
	value, succ := redisHandler.Get("test")
	if !succ {
		println("fail")
	}
	println(value)
}
