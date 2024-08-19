package file

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/sirupsen/logrus"
)

const (
	maxFilesPerDirectory = 1000
	directoryDepth       = 2
)

var (
	cacheHits = promauto.NewCounter(prometheus.CounterOpts{
		Name: "cache_hits_total",
		Help: "The total number of cache hits",
	})
	cacheMisses = promauto.NewCounter(prometheus.CounterOpts{
		Name: "cache_misses_total",
		Help: "The total number of cache misses",
	})
	cacheErrors = promauto.NewCounter(prometheus.CounterOpts{
		Name: "cache_errors_total",
		Help: "The total number of cache errors",
	})
)

type FileDistributionCache struct {
	baseDir string
	mutex   sync.RWMutex
	logger  *logrus.Logger
}

func NewFileDistributionCache(baseDir string, logger *logrus.Logger) (*FileDistributionCache, error) {
	if logger == nil {
		logger = logrus.New()
		logger.SetLevel(logrus.InfoLevel)
	}

	if err := os.MkdirAll(baseDir, 0755); err != nil {
		logger.WithError(err).Error("Failed to create base directory")
		return nil, fmt.Errorf("failed to create base directory: %v", err)
	}

	logger.WithField("baseDir", baseDir).Info("Initialized FileDistributionCache")
	return &FileDistributionCache{baseDir: baseDir, logger: logger}, nil
}

func (fdc *FileDistributionCache) Set(key string, value []byte) error {
	fdc.mutex.Lock()
	defer fdc.mutex.Unlock()

	path := fdc.getFilePath(key)
	dir := filepath.Dir(path)

	fdc.logger.WithFields(logrus.Fields{
		"key":  key,
		"path": path,
		"dir":  dir,
	}).Debug("Attempting to set cache entry")

	if err := os.MkdirAll(dir, 0755); err != nil {
		fdc.logger.WithError(err).WithField("dir", dir).Error("Failed to create directory")
		cacheErrors.Inc()
		return fmt.Errorf("failed to create directory: %v", err)
	}

	if err := ioutil.WriteFile(path, value, 0644); err != nil {
		fdc.logger.WithError(err).WithField("path", path).Error("Failed to write file")
		cacheErrors.Inc()
		return fmt.Errorf("failed to write file: %v", err)
	}

	fdc.logger.WithFields(logrus.Fields{
		"key":  key,
		"path": path,
		"size": len(value),
	}).Info("Successfully set cache entry")

	return nil
}

func (fdc *FileDistributionCache) Get(key string) ([]byte, bool, error) {
	fdc.mutex.RLock()
	defer fdc.mutex.RUnlock()

	path := fdc.getFilePath(key)

	fdc.logger.WithFields(logrus.Fields{
		"key":  key,
		"path": path,
	}).Debug("Attempting to get cache entry")

	data, err := ioutil.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			fdc.logger.WithField("key", key).Debug("Cache miss")
			cacheMisses.Inc()
			return nil, false, nil
		}
		fdc.logger.WithError(err).WithField("path", path).Error("Failed to read file")
		cacheErrors.Inc()
		return nil, false, fmt.Errorf("failed to read file: %v", err)
	}

	fdc.logger.WithFields(logrus.Fields{
		"key":  key,
		"path": path,
		"size": len(data),
	}).Info("Cache hit")
	cacheHits.Inc()
	return data, true, nil
}

func (fdc *FileDistributionCache) Delete(key string) error {
	fdc.mutex.Lock()
	defer fdc.mutex.Unlock()

	path := fdc.getFilePath(key)

	fdc.logger.WithFields(logrus.Fields{
		"key":  key,
		"path": path,
	}).Debug("Attempting to delete cache entry")

	if err := os.Remove(path); err != nil {
		if os.IsNotExist(err) {
			fdc.logger.WithField("key", key).Debug("Cache entry not found for deletion")
			return nil
		}
		fdc.logger.WithError(err).WithField("path", path).Error("Failed to delete file")
		cacheErrors.Inc()
		return fmt.Errorf("failed to delete file: %v", err)
	}

	fdc.logger.WithField("key", key).Info("Successfully deleted cache entry")
	return nil
}

func (fdc *FileDistributionCache) getFilePath(key string) string {
	hash := md5.Sum([]byte(key))
	hexHash := hex.EncodeToString(hash[:])

	var dirs []string
	for i := 0; i < directoryDepth; i++ {
		dirs = append(dirs, hexHash[i*2:i*2+2])
	}

	path := filepath.Join(append([]string{fdc.baseDir}, dirs...)...) + "/" + hexHash
	fdc.logger.WithFields(logrus.Fields{
		"key":  key,
		"hash": hexHash,
		"path": path,
	}).Debug("Generated file path for key")

	return path
}

func (fdc *FileDistributionCache) Cleanup(maxFiles int, maxSize int64) error {
	fdc.logger.Info("Starting cache cleanup")

	var filePaths []string
	var totalSize int64
	var fileCount int

	err := filepath.Walk(fdc.baseDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			fdc.logger.WithError(err).WithField("path", path).Error("Error accessing path")
			return err
		}
		if !info.IsDir() {
			filePaths = append(filePaths, path)
			totalSize += info.Size()
			fileCount++
		}
		return nil
	})

	if err != nil {
		return err
	}

	fdc.logger.WithFields(logrus.Fields{
		"currentFileCount": fileCount,
		"currentTotalSize": totalSize,
	}).Debug("Evaluating file deletion conditions")

	// Delete files if the total file count or size exceeds the threshold
	if fileCount > maxFiles || totalSize > maxSize {
		for _, path := range filePaths {
			if err := os.Remove(path); err != nil {
				fdc.logger.WithError(err).WithField("path", path).Error("Failed to remove file")
				return fmt.Errorf("failed to remove file: %v", err)
			}
			fdc.logger.WithField("path", path).Info("Deleted file during cleanup")
		}
		fdc.logger.WithFields(logrus.Fields{
			"deletedCount": fileCount,
			"totalSize":    totalSize,
		}).Info("Cache cleanup completed")
	} else {
		fdc.logger.WithFields(logrus.Fields{
			"fileCount": fileCount,
			"totalSize": totalSize,
		}).Info("No files deleted during cleanup")
	}

	return nil
}

func (fdc *FileDistributionCache) Stats() (int, int64, error) {
	var fileCount int
	var totalSize int64

	fdc.logger.Info("Collecting cache statistics")

	err := filepath.Walk(fdc.baseDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			fdc.logger.WithError(err).WithField("path", path).Error("Error accessing path during stats collection")
			return err
		}
		if !info.IsDir() {
			fileCount++
			totalSize += info.Size()
		}
		return nil
	})

	fdc.logger.WithFields(logrus.Fields{
		"fileCount": fileCount,
		"totalSize": totalSize,
	}).Info("Cache statistics collected")

	return fileCount, totalSize, err
}
