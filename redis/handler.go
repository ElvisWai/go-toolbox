package redis

import (
	"github.com/redis/go-redis/v9"
	"go-toolbox/logger"
	"golang.org/x/net/context"
	"strings"
	"time"
)

type ModelRedisHandler struct {
	Config
	RedisClient        *redis.Client
	RedisClusterClient *redis.ClusterClient
}

type Config struct {
	Host      string `json:"Host"`
	Password  string `json:"Password"`
	Database  int    `json:"Database"`
	IsCluster bool   `json:"IsCluster"`
	Enable    bool   `json:"Enable"`
}

const (
	NilType      = redis.Nil
	PoolSize int = 800
	MinIdles int = 50
)

func (r *ModelRedisHandler) Set(key string, value interface{}, ex time.Duration) bool {
	if r.IsCluster {
		_, setErr := r.RedisClusterClient.Set(context.Background(), key, value, ex).Result()
		if setErr != nil && setErr != redis.Nil {
			println("Redis 集群 Set 写入错误! 错误原因: " + setErr.Error())
			return false
		}
		return true
	} else {
		_, setErr := r.RedisClient.Set(context.Background(), key, value, ex).Result()
		if setErr != nil && setErr != redis.Nil {
			logger.Logger.Error("Redis Set 写入错误! 错误原因: " + setErr.Error())
			return false
		}
		return true
	}
}

func (r *ModelRedisHandler) Get(key string) (string, bool) {
	if r.IsCluster {
		result, getErr := r.RedisClusterClient.Get(context.Background(), key).Result()
		if getErr != nil && getErr != redis.Nil {
			logger.Logger.Error("Redis 集群 Get 读取错误! 错误原因: " + getErr.Error())
			return "", false
		}
		return result, true
	} else {
		result, getErr := r.RedisClient.Get(context.Background(), key).Result()
		if getErr != nil && getErr != redis.Nil {
			logger.Logger.Error("Redis Get 读取错误! 错误原因: " + getErr.Error())
			return "", false
		}
		return result, true
	}
}

// HashSet accepts values in following formats:
//   - HashSet("myhash", "key1", "value1", "key2", "value2")
//   - HashSet("myhash", []string{"key1", "value1", "key2", "value2"})
//   - HashSet("myhash", map[string]interface{}{"key1": "value1", "key2": "value2"})
//
// Note that it requires Redis v4 for multiple field/value pairs support.
func (r *ModelRedisHandler) HashSet(key string, values ...interface{}) bool {
	if r.IsCluster {
		_, hSetErr := r.RedisClusterClient.HSet(context.Background(), key, values...).Result()
		if hSetErr != nil {
			logger.Logger.Error("Redis 集群 HSet 写入错误! 错误原因: " + hSetErr.Error())
			return false
		}
		return true
	} else {
		_, hSetErr := r.RedisClient.HSet(context.Background(), key, values).Result()
		if hSetErr != nil {
			logger.Logger.Error("Redis HSet 写入错误! 错误原因: " + hSetErr.Error())
			return false
		}
		return true
	}
}

func (r *ModelRedisHandler) HashGet(key, field string) (string, bool) {
	if r.IsCluster {
		result, hGetErr := r.RedisClusterClient.HGet(context.Background(), key, field).Result()
		if hGetErr != nil && hGetErr != redis.Nil {
			logger.Logger.Error("Redis 集群 HGet 读取错误! 错误原因: " + hGetErr.Error())
			return "", false
		}
		return result, true
	} else {
		result, hGetErr := r.RedisClient.HGet(context.Background(), key, field).Result()
		if hGetErr != nil && hGetErr != redis.Nil {
			logger.Logger.Error("Redis HGet 读取错误! 错误原因: " + hGetErr.Error())
			return "", false
		}
		return result, true
	}
}

func (r *ModelRedisHandler) HashMSet(key string, values ...interface{}) bool {
	if r.IsCluster {
		_, hMSetErr := r.RedisClusterClient.HMSet(context.Background(), key, values...).Result()
		if hMSetErr != nil {
			logger.Logger.Error("Redis 集群 HMSet 写入错误! 错误原因: " + hMSetErr.Error())
			return false
		}
		return true
	} else {
		_, hMSetErr := r.RedisClient.HMSet(context.Background(), key, values).Result()
		if hMSetErr != nil {
			logger.Logger.Error("Redis HMSet 写入错误! 错误原因: " + hMSetErr.Error())
			return false
		}
		return true
	}
}

func (r *ModelRedisHandler) HashMGET(key string, fields ...string) ([]interface{}, bool) {
	if r.IsCluster {
		results, hMGetErr := r.RedisClusterClient.HMGet(context.Background(), key, fields...).Result()
		if hMGetErr != nil && hMGetErr != redis.Nil {
			logger.Logger.Error("Redis 集群 HMGet 读取错误! 错误原因: " + hMGetErr.Error())
			return nil, false
		}
		return results, true
	} else {
		results, hMGetErr := r.RedisClient.HMGet(context.Background(), key, fields...).Result()
		if hMGetErr != nil && hMGetErr != redis.Nil {
			logger.Logger.Error("Redis HMGet 读取错误! 错误原因: " + hMGetErr.Error())
			return nil, false
		}
		return results, true
	}
}

func (r *ModelRedisHandler) HashDel(key string, fields ...string) bool {
	if r.IsCluster {
		_, hDelErr := r.RedisClusterClient.HDel(context.Background(), key, fields...).Result()
		if hDelErr != nil {
			logger.Logger.Error("Redis 集群 HDel 删除错误! 错误原因: " + hDelErr.Error())
			return false
		}
		return true
	} else {
		_, hDelErr := r.RedisClient.HDel(context.Background(), key, fields...).Result()
		if hDelErr != nil {
			logger.Logger.Error("Redis HDel 删除错误! 错误原因: " + hDelErr.Error())
			return false
		}
		return true
	}
}

func (r *ModelRedisHandler) HashLen(key string) int64 {
	if r.IsCluster {
		hashLen, hLenErr := r.RedisClusterClient.HLen(context.Background(), key).Result()
		if hLenErr != nil {
			logger.Logger.Error("Redis 集群 HLen 获取长度错误! 错误原因: " + hLenErr.Error())
			return -1
		}
		return hashLen
	} else {
		hashLen, hLenErr := r.RedisClient.HLen(context.Background(), key).Result()
		if hLenErr != nil {
			logger.Logger.Error("Redis HLen 获取长度错误! 错误原因: " + hLenErr.Error())
			return -1
		}
		return hashLen
	}
}

func (r *ModelRedisHandler) GetList(key string, start, stop int64) ([]string, bool) {
	if r.IsCluster {
		result, lRangeErr := r.RedisClusterClient.LRange(context.Background(), key, start, stop).Result()
		if lRangeErr != nil {
			logger.Logger.Error("Redis 集群 LRANGE 获取列表错误! 错误原因: " + lRangeErr.Error())
			return nil, false
		}
		return result, true
	} else {
		result, lRangeErr := r.RedisClient.LRange(context.Background(), key, start, stop).Result()
		if lRangeErr != nil {
			logger.Logger.Error("Redis LRANGE 获取列表错误! 错误原因: " + lRangeErr.Error())
			return nil, false
		}
		return result, true
	}
}

func (r *ModelRedisHandler) EmptyList(key string) bool {
	if r.IsCluster {
		_, lTrimErr := r.RedisClusterClient.LTrim(context.Background(), key, -1, 0).Result()
		if lTrimErr != nil {
			logger.Logger.Error("Redis 集群 LTRIM 获取列表错误! 错误原因: " + lTrimErr.Error())
			return false
		}
		return true
	} else {
		_, lTrimErr := r.RedisClient.LTrim(context.Background(), key, -1, 0).Result()
		if lTrimErr != nil {
			logger.Logger.Error("Redis LTRIM 获取列表错误! 错误原因: " + lTrimErr.Error())
			return false
		}
		return true
	}
}

func (r *ModelRedisHandler) AppendList(key string, value interface{}) bool {
	if r.IsCluster {
		_, appendErr := r.RedisClusterClient.LPush(context.Background(), key, value).Result()
		if appendErr != nil {
			logger.Logger.Error("Redis 集群 LPUSH 写入列表错误! 错误原因: " + appendErr.Error())
			return false
		}
		return true
	} else {
		_, appendErr := r.RedisClient.LPush(context.Background(), key, value).Result()
		if appendErr != nil {
			logger.Logger.Error("Redis LPUSH 写入列表错误! 错误原因: " + appendErr.Error())
			return false
		}
		return true
	}
}

func (r *ModelRedisHandler) BFAdd(key string, value string) (bool, bool) {
	// TxPipeline 的性能会比 Pipeline 好
	if r.IsCluster {
		inserted, err := r.RedisClusterClient.Do(context.Background(), "BF.ADD", key, value).Bool()
		if err != nil {
			logger.Logger.Error("Redis 集群 BFAdd 写入布隆过滤器错误! 错误原因: " + err.Error())
			return false, false
		}
		return inserted, true
	} else {
		inserted, err := r.RedisClient.Do(context.Background(), "BF.ADD", key, value).Bool()
		if err != nil {
			logger.Logger.Error("Redis BFAdd 写入布隆过滤器错误! 错误原因: " + err.Error())
			return false, false
		}
		return inserted, true
	}
}

func (r *ModelRedisHandler) BFExists(key string, value string) bool {
	if r.IsCluster {
		inserted, err := r.RedisClusterClient.Do(context.Background(), "BF.Exists", key, value).Bool()
		if err != nil {
			//panic(err)
			logger.Logger.Error("Redis 集群 BFExists 查询布隆过滤器错误! 错误原因: " + err.Error())
			return false
		}
		return inserted
	} else {
		inserted, err := r.RedisClient.Do(context.Background(), "BF.Exists", key, value).Bool()
		if err != nil {
			logger.Logger.Error("Redis BFExists 查询布隆过滤器错误! 错误原因: " + err.Error())
			return false
		}
		return inserted
	}
}

// Pipeline pipeline
func (r *ModelRedisHandler) Pipeline() (redis.Pipeliner, context.Context) {
	// TxPipeline 的性能会比 Pipeline 好
	if r.IsCluster {
		return r.RedisClusterClient.Pipeline(), context.Background()
	} else {
		return r.RedisClient.Pipeline(), context.Background()
	}
}

// PipelineExecute pipeline 执行
func (r *ModelRedisHandler) PipelineExecute(pipe redis.Pipeliner, ctx context.Context) ([]redis.Cmder, error) {
	return pipe.Exec(ctx)
}

// ShutdownRedisHandler 关闭 Redis 连接
func (r *ModelRedisHandler) ShutdownRedisHandler() error {
	if r.IsCluster {
		return r.RedisClusterClient.Close()
	} else {
		return r.RedisClient.Close()
	}
}

func (r *ModelRedisHandler) initRedisClusterClient() {
	if !strings.Contains(r.Host, ",") {
		logger.Logger.Fatal(logger.GetLogPrefix("") + "Redis 集群地址请按英文逗号分割!")
	}
	client := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:        strings.Split(r.Host, ","),
		Password:     r.Password,
		PoolSize:     PoolSize,
		MinIdleConns: MinIdles,
	})
	pingErr := client.Ping(context.Background()).Err()
	if pingErr != nil {
		logger.Logger.Fatal(logger.GetLogPrefix("") + "Redis 集群连接失败! 错误原因: " + pingErr.Error())
	}
	r.RedisClusterClient = client
}

func (r *ModelRedisHandler) initRedisClient() {
	client := redis.NewClient(&redis.Options{
		Addr:         r.Host,
		Password:     r.Password,
		DB:           r.Database,
		PoolSize:     PoolSize,
		MinIdleConns: MinIdles,
	})
	pingErr := client.Ping(context.Background()).Err()
	if pingErr != nil {
		logger.Logger.Fatal(logger.GetLogPrefix("") + "Redis 连接失败! 错误原因: " + pingErr.Error())
	}
	r.RedisClient = client
}

func (r *ModelRedisHandler) initRedisHandler() {
	if r.IsCluster {
		r.initRedisClusterClient()
		logger.Logger.Info(logger.GetLogPrefix("") + "Redis 连接成功! 当前模式: Redis 集群")
	} else {
		r.initRedisClient()
		logger.Logger.Info(logger.GetLogPrefix("") + "Redis 连接成功! 当前模式: Redis 单点")
	}
}

func NewRedisHandler(redisConf *Config) *ModelRedisHandler {
	redisClient := &ModelRedisHandler{
		Config{
			Host:      redisConf.Host,
			Password:  redisConf.Password,
			Database:  redisConf.Database,
			IsCluster: redisConf.IsCluster,
		},
		nil,
		nil,
	}
	redisClient.initRedisHandler()
	return redisClient
}
