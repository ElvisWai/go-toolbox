package go_toolbox

import (
	"testing"
	"time"
)

var config = RedisConf{
	Host:      ":6379",
	Password:  "",
	Database:  0,
	IsCluster: true,
	Enable:    true,
}

func TestGSet(t *testing.T) {
	redisHandler := NewRedisHandler(&config)
	redisHandler.Set("tests", "a", time.Second)
	value, success := redisHandler.Get("tests")
	if !success {
		println("fail")
	}
	println(value)

}
