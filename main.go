package main

import (
	"crypto/md5"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"redis-server/file-dist"
	"strconv"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/btree"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sirupsen/logrus"
)

type LogLevel int

const (
	ERROR LogLevel = iota
	INFO
	DEBUG
)

type LogEntry struct {
	Timestamp string  `json:"timestamp"`
	Level     string  `json:"level"`
	Message   string  `json:"message"`
	Data      LogData `json:"data,omitempty"`
}

type LogData map[string]interface{}

type ZSetItem struct {
	Member string
	Score  float64
}

func (a ZSetItem) Less(b btree.Item) bool {
	other := b.(ZSetItem)
	if a.Score == other.Score {
		return a.Member < other.Member
	}
	return a.Score < other.Score
}

type ZSet struct {
	Tree   *btree.BTree
	Scores map[string]float64
}

type RedisValue struct {
	Data       interface{} `json:"data"`
	Expiration *time.Time  `json:"expiration,omitempty"`
	Type       string      `json:"type"`
}

type RedisServer struct {
	data        map[string]RedisValue
	mutex       sync.RWMutex
	logger      *logrus.Logger
	persistence *Persistence
	metrics     *Metrics
	logFile     *os.File
	cacheDir    string
	cacheSystem *file.FileDistributionCache
}

type Persistence struct {
	filePath string
}

type Metrics struct {
	commandsProcessed *prometheus.CounterVec
	activeConnections prometheus.Gauge
}

func NewRedisServer(logPath, cacheDir string) (*RedisServer, error) {
	logger := logrus.New()
	logger.SetOutput(os.Stdout)
	logger.SetLevel(logrus.InfoLevel)
	logger.SetFormatter(&logrus.JSONFormatter{})

	cacheSystem, err := file.NewFileDistributionCache("./cache", logger)
	if err != nil {
		log.Fatalf("Failed to initialize cache: %v", err)
	}

	persistence := &Persistence{
		filePath: "redis_data.json",
	}

	metrics := &Metrics{
		commandsProcessed: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "redis_commands_processed_total",
				Help: "The total number of processed Redis commands",
			},
			[]string{"command"},
		),
		activeConnections: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name: "redis_active_connections",
				Help: "The number of active connections",
			},
		),
	}

	prometheus.MustRegister(metrics.commandsProcessed)
	prometheus.MustRegister(metrics.activeConnections)

	logFile, err := os.OpenFile(logPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open log file: %v", err)
	}

	err = os.MkdirAll(cacheDir, os.ModePerm)
	if err != nil {
		return nil, fmt.Errorf("failed to create cache directory: %v", err)
	}

	server := &RedisServer{
		data:        make(map[string]RedisValue),
		logger:      logger,
		persistence: persistence,
		metrics:     metrics,
		logFile:     logFile,
		cacheDir:    cacheDir,
		cacheSystem: cacheSystem,
	}

	server.loadData()

	return server, nil
}

func (s *RedisServer) loadData() {
	data, err := os.ReadFile(s.persistence.filePath)
	if err != nil {
		if os.IsNotExist(err) {
			s.logger.Info("No existing data file found. Starting with empty data.")
			return
		}
		s.logger.Warnf("Failed to load data: %v", err)
		return
	}
	if len(data) == 0 {
		s.logger.Info("Data file is empty. Starting with empty data.")
		return
	}
	err = json.Unmarshal(data, &s.data)
	if err != nil {
		s.logger.Errorf("Failed to unmarshal data: %v", err)
	}

}

func (s *RedisServer) log(level LogLevel, message string, data LogData) {
	levelStr := "INFO"
	switch level {
	case ERROR:
		levelStr = "ERROR"
	case DEBUG:
		levelStr = "DEBUG"
	}

	entry := LogEntry{
		Timestamp: time.Now().Format(time.RFC3339),
		Level:     levelStr,
		Message:   message,
		Data:      data,
	}

	jsonEntry, err := json.Marshal(entry)
	if err != nil {
		fmt.Printf("Error marshaling log entry: %v\n", err)
		return
	}

	s.logFile.Write(jsonEntry)
	s.logFile.Write([]byte("\n"))
}

func (s *RedisServer) saveToCache(key string, value RedisValue) {
	// Create a hash of the key to use as the filename
	hasher := md5.New()
	hasher.Write([]byte(key))
	hash := hex.EncodeToString(hasher.Sum(nil))

	subdir := hash[:2]
	cacheDir := filepath.Join(s.cacheDir, subdir)

	os.MkdirAll(cacheDir, os.ModePerm)

	cacheFile := filepath.Join(cacheDir, hash)

	data, err := json.Marshal(value)
	if err != nil {
		s.log(ERROR, "Failed to marshal cache data", LogData{"key": key, "error": err.Error()})
		return
	}

	// Attempt to write the file with multiple retries
	maxRetries := 3
	for i := 0; i < maxRetries; i++ {
		err = ioutil.WriteFile(cacheFile, data, 0644)
		if err == nil {
			break
		}
		s.log(DEBUG, fmt.Sprintf("Retry %d: Failed to save cache file", i+1), LogData{"key": key, "error": err.Error()})
		time.Sleep(time.Millisecond * 100) // Wait a bit before retrying
	}

	if err != nil {
		s.log(ERROR, "Failed to save cache file after retries", LogData{"key": key, "error": err.Error()})
	}
}

func (s *RedisServer) Set(key string, value interface{}, expiration *time.Time) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	redisValue := RedisValue{Data: value, Expiration: expiration, Type: "string"}
	s.data[key] = redisValue

	data, err := json.Marshal(redisValue)
	if err != nil {
		s.logger.WithFields(logrus.Fields{
			"key":   key,
			"error": err.Error(),
		}).Error("Failed to marshal data")
		return
	}

	if err := s.cacheSystem.Set(key, data); err != nil {
		s.logger.WithFields(logrus.Fields{
			"key":   key,
			"error": err.Error(),
		}).Error("Failed to save to cache")
	}

	s.logger.WithField("key", key).Info("SET operation completed")
}

func (s *RedisServer) MSet(data map[string]string) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	for key, value := range data {
		redisValue := RedisValue{Data: value, Type: "string"}
		s.data[key] = redisValue
		s.log(INFO, "MSET operation", LogData{"key": key})

		// Save to cache file
		go s.saveToCache(key, redisValue)
	}
}

func (s *RedisServer) Get(key string) (interface{}, bool) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	data, exists, err := s.cacheSystem.Get(key)
	if err != nil {
		s.logger.WithFields(logrus.Fields{
			"key":   key,
			"error": err.Error(),
		}).Error("Failed to retrieve from cache")
		return nil, false
	}
	if !exists {
		s.logger.WithField("key", key).Debug("GET operation - key not found")
		return nil, false
	}

	var value RedisValue
	if err := json.Unmarshal(data, &value); err != nil {
		s.logger.WithFields(logrus.Fields{
			"key":   key,
			"error": err.Error(),
		}).Error("Failed to unmarshal data")
		return nil, false
	}

	if value.Expiration != nil && time.Now().After(*value.Expiration) {
		s.cacheSystem.Delete(key)
		s.logger.WithField("key", key).Debug("GET operation - key expired")
		return nil, false
	}

	s.logger.WithField("key", key).Info("GET operation completed")
	return value.Data, true
}

func (s *RedisServer) ZAdd(key string, score float64, member string) int {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	zset, exists := s.data[key]
	if !exists || zset.Type != "zset" {
		zset = RedisValue{
			Data: &ZSet{
				Tree:   btree.New(32),
				Scores: make(map[string]float64),
			},
			Type: "zset",
		}
		s.data[key] = zset
	}

	zsetData, ok := zset.Data.(*ZSet)
	if !ok {
		zsetData = &ZSet{
			Tree:   btree.New(32),
			Scores: make(map[string]float64),
		}
		zset.Data = zsetData
		s.data[key] = zset
	}

	oldScore, exists := zsetData.Scores[member]
	if exists {
		zsetData.Tree.Delete(ZSetItem{Member: member, Score: oldScore})
	}

	zsetData.Tree.ReplaceOrInsert(ZSetItem{Member: member, Score: score})
	zsetData.Scores[member] = score

	if exists {
		return 0
	}
	return 1
}

func (s *RedisServer) ZRange(key string, start, stop int) ([]string, bool) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	zset, exists := s.data[key]
	if !exists || zset.Type != "zset" {
		return nil, false
	}

	zsetData, ok := zset.Data.(*ZSet)
	if !ok {
		return nil, false
	}

	size := zsetData.Tree.Len()

	if start < 0 {
		start = size + start
	}
	if stop < 0 {
		stop = size + stop
	}
	if start < 0 {
		start = 0
	}
	if stop >= size {
		stop = size - 1
	}
	if start > stop {
		return []string{}, true
	}

	result := make([]string, 0, stop-start+1)
	i := 0
	zsetData.Tree.Ascend(func(item btree.Item) bool {
		if i >= start && i <= stop {
			result = append(result, item.(ZSetItem).Member)
		}
		i++
		return i <= stop
	})

	return result, true
}

func (s *RedisServer) SAdd(key string, members ...string) int {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	set, exists := s.data[key]
	if !exists || set.Type != "set" {
		set = RedisValue{Data: make(map[string]bool), Type: "set"}
		s.data[key] = set
	}

	setData, ok := set.Data.(map[string]bool)
	if !ok {
		setData = make(map[string]bool)
		set.Data = setData
		s.data[key] = set
	}

	added := 0
	for _, member := range members {
		if _, exists := setData[member]; !exists {
			setData[member] = true
			added++
		}
	}

	return added
}

func (s *RedisServer) SMembers(key string) ([]string, bool) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	set, exists := s.data[key]
	if !exists || set.Type != "set" {
		return nil, false
	}

	setData, ok := set.Data.(map[string]bool)
	if !ok {
		return nil, false
	}

	members := make([]string, 0, len(setData))
	for member := range setData {
		members = append(members, member)
	}

	return members, true
}

func (s *RedisServer) ZRangeByScore(key string, min, max float64) ([]string, bool) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	zset, exists := s.data[key]
	if !exists || zset.Type != "zset" {
		return nil, false
	}

	zsetData, ok := zset.Data.(*ZSet)
	if !ok {
		return nil, false
	}

	var result []string
	zsetData.Tree.AscendRange(ZSetItem{Score: min}, ZSetItem{Score: max}, func(item btree.Item) bool {
		result = append(result, item.(ZSetItem).Member)
		return true
	})

	return result, true
}

func (s *RedisServer) Delete(key string) bool {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	_, exists := s.data[key]
	delete(s.data, key)

	if err := s.cacheSystem.Delete(key); err != nil {
		s.logger.WithFields(logrus.Fields{
			"key":   key,
			"error": err.Error(),
		}).Error("Failed to delete from cache")
	}

	s.logger.WithField("key", key).Info("DELETE operation completed")
	return exists
}

func (s *RedisServer) startMaintenanceTask() {
	go func() {
		for {
			if err := s.cacheSystem.Cleanup(1000, 60000); err != nil { // E.g., max 10 files or 100 KB
				s.logger.WithError(err).Error("Cache cleanup failed")
			}

			fileCount, totalSize, err := s.cacheSystem.Stats()
			if err != nil {
				s.logger.WithError(err).Error("Failed to get cache stats")
			} else {
				s.logger.WithFields(logrus.Fields{
					"fileCount": fileCount,
					"totalSize": totalSize,
				}).Info("Cache stats collected")
			}

			time.Sleep(2 * time.Hour) // Run the maintenance task every 2 minutes for testing
		}
	}()
}

func (s *RedisServer) Expire(key string, seconds int) bool {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	value, exists := s.data[key]
	if !exists {
		return false
	}
	expiration := time.Now().Add(time.Duration(seconds) * time.Second)
	value.Expiration = &expiration
	s.data[key] = value
	s.saveData()
	return true
}

func (s *RedisServer) TTL(key string) int {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	value, exists := s.data[key]
	if !exists {
		return -2 // Key does not exist
	}
	if value.Expiration == nil {
		return -1 // Key exists but has no associated expire
	}
	ttl := time.Until(*value.Expiration)
	if ttl < 0 {
		delete(s.data, key)
		s.saveData()
		return -2
	}
	return int(ttl.Seconds())
}

func (s *RedisServer) HSet(key, field string, value interface{}) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	hash, exists := s.data[key]
	if !exists || hash.Type != "hash" {
		hash = RedisValue{Data: make(map[string]interface{}), Type: "hash"}
	}
	hash.Data.(map[string]interface{})[field] = value
	s.data[key] = hash
	s.saveData()
}

func (s *RedisServer) HGet(key, field string) (interface{}, bool) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	hash, exists := s.data[key]
	if !exists || hash.Type != "hash" {
		return nil, false
	}
	value, exists := hash.Data.(map[string]interface{})[field]
	return value, exists
}

func (s *RedisServer) HGetAll(key string) (map[string]interface{}, bool) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	hash, exists := s.data[key]
	if !exists || hash.Type != "hash" {
		return nil, false
	}
	return hash.Data.(map[string]interface{}), true
}

func (s *RedisServer) LPush(key string, values ...interface{}) int {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	list, exists := s.data[key]
	if !exists || list.Type != "list" {
		list = RedisValue{Data: make([]interface{}, 0), Type: "list"}
	}
	list.Data = append(values, list.Data.([]interface{})...)
	s.data[key] = list
	s.saveData()
	return len(list.Data.([]interface{}))
}

func (s *RedisServer) RPop(key string) (interface{}, bool) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	list, exists := s.data[key]
	if !exists || list.Type != "list" {
		return nil, false
	}
	data := list.Data.([]interface{})
	if len(data) == 0 {
		return nil, false
	}
	value := data[len(data)-1]
	list.Data = data[:len(data)-1]
	s.data[key] = list
	s.saveData()
	return value, true
}

func (s *RedisServer) ZRangeWithScores(key string, start, stop int) ([]ZSetItem, bool) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	zset, exists := s.data[key]
	if !exists || zset.Type != "zset" {
		return nil, false
	}

	zsetData := zset.Data.(*ZSet)
	size := zsetData.Tree.Len()

	if start < 0 {
		start = size + start
	}
	if stop < 0 {
		stop = size + stop
	}
	if start < 0 {
		start = 0
	}
	if stop >= size {
		stop = size - 1
	}
	if start > stop {
		return []ZSetItem{}, true
	}

	result := make([]ZSetItem, 0, stop-start+1)
	i := 0
	zsetData.Tree.Ascend(func(item btree.Item) bool {
		if i >= start && i <= stop {
			result = append(result, item.(ZSetItem))
		}
		i++
		return i <= stop
	})

	return result, true
}

// Add this method to support reverse order range queries
func (s *RedisServer) ZRevRange(key string, start, stop int) ([]string, bool) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	zset, exists := s.data[key]
	if !exists || zset.Type != "zset" {
		return nil, false
	}

	zsetData := zset.Data.(*ZSet)
	size := zsetData.Tree.Len()

	if start < 0 {
		start = size + start
	}
	if stop < 0 {
		stop = size + stop
	}
	if start < 0 {
		start = 0
	}
	if stop >= size {
		stop = size - 1
	}
	if start > stop {
		return []string{}, true
	}

	result := make([]string, 0, stop-start+1)
	i := 0
	zsetData.Tree.Descend(func(item btree.Item) bool {
		if i >= start && i <= stop {
			result = append(result, item.(ZSetItem).Member)
		}
		i++
		return i <= stop
	})

	return result, true
}

func (s *RedisServer) ZRank(key, member string) (int, bool) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	zset, exists := s.data[key]
	if !exists || zset.Type != "zset" {
		return 0, false
	}

	zsetData := zset.Data.(*ZSet)
	score, exists := zsetData.Scores[member]
	if !exists {
		return 0, false
	}

	rank := 0
	zsetData.Tree.AscendLessThan(ZSetItem{Member: member, Score: score}, func(item btree.Item) bool {
		rank++
		return true
	})

	return rank, true
}

func (s *RedisServer) saveData() {
	data, err := json.Marshal(s.data)
	if err != nil {
		s.logger.Errorf("Failed to marshal data: %v", err)
		return
	}
	err = ioutil.WriteFile(s.persistence.filePath, data, 0644)
	if err != nil {
		s.logger.Errorf("Failed to save data: %v", err)
	}
}

func (s *RedisServer) Incr(key string) (int64, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	value, exists := s.data[key]
	if !exists || value.Type != "string" {
		s.data[key] = RedisValue{Data: "1", Type: "string"}
		return 1, nil
	}

	intVal, err := strconv.ParseInt(value.Data.(string), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("value is not an integer")
	}

	intVal++
	s.data[key] = RedisValue{Data: strconv.FormatInt(intVal, 10), Type: "string"}
	return intVal, nil
}

func (s *RedisServer) Decr(key string) (int64, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	value, exists := s.data[key]
	if !exists || value.Type != "string" {
		s.data[key] = RedisValue{Data: "-1", Type: "string"}
		return -1, nil
	}

	intVal, err := strconv.ParseInt(value.Data.(string), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("value is not an integer")
	}

	intVal--
	s.data[key] = RedisValue{Data: strconv.FormatInt(intVal, 10), Type: "string"}
	return intVal, nil
}

func authMiddleware(c *gin.Context) {
	// Implement your authentication logic here
	// For example, check for an API key in the header
	apiKey := c.GetHeader("X-API-Key")
	if apiKey != "your-secret-api-key" {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "Unauthorized"})
		c.Abort()
		return
	}
	c.Next()
}

// implement data sharding and replication logic

func main() {
	server, err := NewRedisServer("redis_server.log", "cache")
	if err != nil {
		fmt.Printf("Failed to create server: %v\n", err)
		return
	}

	server.startMaintenanceTask()

	r := gin.New()
	r.Use(gin.Recovery())

	gin.SetMode(gin.ReleaseMode)

	r.Use(func(c *gin.Context) {
		start := time.Now()
		path := c.Request.URL.Path

		c.Next()

		end := time.Now()
		latency := end.Sub(start)

		server.logger.WithFields(logrus.Fields{
			"status":     c.Writer.Status(),
			"method":     c.Request.Method,
			"path":       path,
			"ip":         c.ClientIP(),
			"latency":    latency,
			"user-agent": c.Request.UserAgent(),
		}).Info("Request processed")
	})

	// Use Gin's Logger middleware
	r.Use(gin.Logger())

	// Use Gin's Recovery middleware
	r.Use(gin.Recovery())

	// Custom middleware for tracking active connections
	r.Use(func(c *gin.Context) {
		server.metrics.activeConnections.Inc()
		c.Next()
		server.metrics.activeConnections.Dec()
	})

	// Authentication middleware
	authMiddleware := func(c *gin.Context) {
		apiKey := c.GetHeader("X-API-Key")
		if apiKey != "your-secret-api-key" {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "Unauthorized"})
			c.Abort()
			return
		}
		c.Next()
	}

	// Authenticated routes
	auth := r.Group("/")
	auth.Use(authMiddleware)
	{
		auth.POST("/set", func(c *gin.Context) {
			key := c.Query("key")
			value := c.Query("value")
			expStr := c.Query("exp")

			var expiration *time.Time
			if expStr != "" {
				exp, err := strconv.Atoi(expStr)
				if err != nil {
					c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid expiration time"})
					return
				}
				t := time.Now().Add(time.Duration(exp) * time.Second)
				expiration = &t
			}

			server.Set(key, value, expiration)
			server.metrics.commandsProcessed.With(prometheus.Labels{"command": "SET"}).Inc()
			c.JSON(http.StatusOK, gin.H{"message": "OK"})
			server.logger.WithFields(logrus.Fields{
				"command": "SET",
				"key":     key,
				"value":   value,
			}).Info("Command executed successfully")
		})

		auth.GET("/get/:key", func(c *gin.Context) {
			key := c.Param("key")
			value, exists := server.Get(key)
			server.metrics.commandsProcessed.With(prometheus.Labels{"command": "GET"}).Inc()
			if !exists {
				c.JSON(http.StatusNotFound, gin.H{"error": "Key not found"})
				server.logger.WithFields(logrus.Fields{
					"command": "GET",
					"key":     key,
				}).Warn("Key not found")
				return
			}
			c.JSON(http.StatusOK, gin.H{"value": value})
			server.logger.WithFields(logrus.Fields{
				"command": "GET",
				"key":     key,
				"value":   value,
			}).Info("Command executed successfully")
		})

		r.POST("/mset", func(c *gin.Context) {
			var data map[string]string
			if err := c.BindJSON(&data); err != nil {
				c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid data format"})
				return
			}
			server.MSet(data)
			c.JSON(http.StatusOK, gin.H{"message": "Batch set successful"})
		})

		// r.POST("/mset", func(c *gin.Context) {
		// 	var batchData map[string]string
		// 	if err := c.BindJSON(&batchData); err != nil {
		// 		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid data format"})
		// 		return
		// 	}

		// 	for key, value := range batchData {
		// 		server.Set(key, value, nil)
		// 	}

		// 	c.JSON(http.StatusOK, gin.H{"message": "Batch set successful"})
		// })

		auth.DELETE("/del/:key", func(c *gin.Context) {
			key := c.Param("key")
			deleted := server.Delete(key)
			server.metrics.commandsProcessed.With(prometheus.Labels{"command": "DEL"}).Inc()
			if deleted {
				c.JSON(http.StatusOK, gin.H{"message": "Key deleted"})
				server.logger.WithFields(logrus.Fields{
					"command": "DEL",
					"key":     key,
				}).Info("Key deleted successfully")
			} else {
				c.JSON(http.StatusNotFound, gin.H{"error": "Key not found"})
				server.logger.WithFields(logrus.Fields{
					"command": "DEL",
					"key":     key,
				}).Warn("Key not found for deletion")
			}
		})

		auth.POST("/expire", func(c *gin.Context) {
			key := c.Query("key")
			seconds, _ := strconv.Atoi(c.Query("seconds"))
			success := server.Expire(key, seconds)
			c.JSON(http.StatusOK, gin.H{"success": success})
		})

		auth.GET("/ttl/:key", func(c *gin.Context) {
			key := c.Param("key")
			ttl := server.TTL(key)
			c.JSON(http.StatusOK, gin.H{"ttl": ttl})
		})

		auth.POST("/hset", func(c *gin.Context) {
			key := c.Query("key")
			field := c.Query("field")
			value := c.Query("value")
			server.HSet(key, field, value)
			c.JSON(http.StatusOK, gin.H{"message": "OK"})
		})

		auth.GET("/hget/:key/:field", func(c *gin.Context) {
			key := c.Param("key")
			field := c.Param("field")
			value, exists := server.HGet(key, field)
			if !exists {
				c.JSON(http.StatusNotFound, gin.H{"error": "Key or field not found"})
				return
			}
			c.JSON(http.StatusOK, gin.H{"value": value})
		})

		auth.GET("/hgetall/:key", func(c *gin.Context) {
			key := c.Param("key")
			hash, exists := server.HGetAll(key)
			if !exists {
				c.JSON(http.StatusNotFound, gin.H{"error": "Key not found"})
				return
			}
			c.JSON(http.StatusOK, hash)
		})

		auth.POST("/lpush", func(c *gin.Context) {
			key := c.Query("key")
			values := c.QueryArray("value")
			length := server.LPush(key, values)
			c.JSON(http.StatusOK, gin.H{"length": length})
		})

		auth.POST("/rpop/:key", func(c *gin.Context) {
			key := c.Param("key")
			value, exists := server.RPop(key)
			if !exists {
				c.JSON(http.StatusNotFound, gin.H{"error": "Key not found or list is empty"})
				return
			}
			c.JSON(http.StatusOK, gin.H{"value": value})
		})

		auth.POST("/sadd", func(c *gin.Context) {
			key := c.Query("key")
			members := c.QueryArray("member")
			added := server.SAdd(key, members...)
			c.JSON(http.StatusOK, gin.H{"added": added})
		})

		auth.GET("/smembers/:key", func(c *gin.Context) {
			key := c.Param("key")
			members, exists := server.SMembers(key)
			if !exists {
				c.JSON(http.StatusNotFound, gin.H{"error": "Key not found"})
				return
			}
			c.JSON(http.StatusOK, gin.H{"members": members})
		})

		auth.POST("/zadd", func(c *gin.Context) {
			key := c.Query("key")
			score, _ := strconv.ParseFloat(c.Query("score"), 64)
			member := c.Query("member")
			added := server.ZAdd(key, score, member)
			c.JSON(http.StatusOK, gin.H{"added": added})
		})

		auth.GET("/zrange/:key/:start/:stop", func(c *gin.Context) {
			key := c.Param("key")
			start, _ := strconv.Atoi(c.Param("start"))
			stop, _ := strconv.Atoi(c.Param("stop"))
			members, exists := server.ZRange(key, start, stop)
			if !exists {
				c.JSON(http.StatusNotFound, gin.H{"error": "Key not found"})
				return
			}
			c.JSON(http.StatusOK, gin.H{"members": members})
		})

		auth.GET("/zrangebyscore/:key/:min/:max", func(c *gin.Context) {
			key := c.Param("key")
			min, _ := strconv.ParseFloat(c.Param("min"), 64)
			max, _ := strconv.ParseFloat(c.Param("max"), 64)
			members, exists := server.ZRangeByScore(key, min, max)
			if !exists {
				c.JSON(http.StatusNotFound, gin.H{"error": "Key not found"})
				return
			}
			c.JSON(http.StatusOK, gin.H{"members": members})
		})

		auth.GET("/zrevrange/:key/:start/:stop", func(c *gin.Context) {
			key := c.Param("key")
			start, _ := strconv.Atoi(c.Param("start"))
			stop, _ := strconv.Atoi(c.Param("stop"))
			members, exists := server.ZRevRange(key, start, stop)
			if !exists {
				c.JSON(http.StatusNotFound, gin.H{"error": "Key not found"})
				return
			}
			c.JSON(http.StatusOK, gin.H{"members": members})
		})

		auth.GET("/zrank/:key/:member", func(c *gin.Context) {
			key := c.Param("key")
			member := c.Param("member")
			rank, exists := server.ZRank(key, member)
			if !exists {
				c.JSON(http.StatusNotFound, gin.H{"error": "Key or member not found"})
				return
			}
			c.JSON(http.StatusOK, gin.H{"rank": rank})
		})

		auth.GET("/zrangewithscores/:key/:start/:stop", func(c *gin.Context) {
			key := c.Param("key")
			start, _ := strconv.Atoi(c.Param("start"))
			stop, _ := strconv.Atoi(c.Param("stop"))
			items, exists := server.ZRangeWithScores(key, start, stop)
			if !exists {
				c.JSON(http.StatusNotFound, gin.H{"error": "Key not found"})
				return
			}
			c.JSON(http.StatusOK, gin.H{"items": items})
		})

		auth.POST("/incr/:key", func(c *gin.Context) {
			key := c.Param("key")
			value, err := server.Incr(key)
			if err != nil {
				c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
				return
			}
			c.JSON(http.StatusOK, gin.H{"value": value})
		})

		auth.POST("/decr/:key", func(c *gin.Context) {
			key := c.Param("key")
			value, err := server.Decr(key)
			if err != nil {
				c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
				return
			}
			c.JSON(http.StatusOK, gin.H{"value": value})
		})
	}

	// Public routes
	r.GET("/ping", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"message": "PONG"})
		server.logger.Info("PING command received")
	})

	// Metrics endpoint
	r.GET("/metrics", gin.WrapH(promhttp.Handler()))

	// Configure TLS
	tlsConfig := &tls.Config{
		MinVersion:               tls.VersionTLS12,
		CurvePreferences:         []tls.CurveID{tls.CurveP521, tls.CurveP384, tls.CurveP256},
		PreferServerCipherSuites: true,
		CipherSuites: []uint16{
			tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
			tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA,
			tls.TLS_RSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_RSA_WITH_AES_256_CBC_SHA,
		},
	}

	// Create a new http.Server with the Gin handler and TLS config
	httpServer := &http.Server{
		Addr:      ":6379",
		Handler:   r,
		TLSConfig: tlsConfig,
	}

	server.logger.Info("Starting Redis-like server on :6379")
	server.logger.Fatal(httpServer.ListenAndServeTLS("server.crt", "server.key"))
}
