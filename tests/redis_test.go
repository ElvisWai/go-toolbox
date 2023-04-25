package tests

import (
	"go-toolbox/redis"
	"testing"
	"time"
)

var config = redis.Config{
	Host:      "",
	Password:  "",
	Database:  0,
	IsCluster: true,
	Enable:    true,
}

func TestGSet(t *testing.T) {
	redisHandler := redis.NewRedisHandler(&config)
	redisHandler.Set("tests", "a", time.Second)
	value, success := redisHandler.Get("tests")
	if !success {
		println("fail")
	}
	println(value)

}
