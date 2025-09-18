package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

type Config struct {
	KeyPattern  string
	RedisAddrs  []string
	Concurrency int
	BatchSize   int
	DryRun      bool
}

func main() {
	var addrs string
	var keyPattern string
	var dryRun bool
	var concurrency int
	var batchSize int

	flag.StringVar(&addrs, "a", "localhost:6379", "Redis cluster addresses (comma-separated)")
	flag.StringVar(&keyPattern, "k", "", "Key pattern to match (required)")
	flag.BoolVar(&dryRun, "dry-run", false, "Dry run mode (only show keys, don't delete)")
	flag.IntVar(&concurrency, "c", 10, "Concurrency level for scanning")
	flag.IntVar(&batchSize, "b", 1000, "Batch size for deletion")
	flag.Parse()

	if keyPattern == "" {
		fmt.Println("Error: key pattern is required")
		flag.Usage()
		os.Exit(1)
	}

	config := Config{
		RedisAddrs:  strings.Split(addrs, ","),
		KeyPattern:  keyPattern,
		DryRun:      dryRun,
		Concurrency: concurrency,
		BatchSize:   batchSize,
	}

	if err := run(config); err != nil {
		log.Fatalf("Error: %v", err)
	}
}

func run(config Config) error {
	ctx := context.Background()

	// Create Redis cluster client
	rdb := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:        config.RedisAddrs,
		MaxRedirects: 5,
		PoolSize:     config.Concurrency * 2,
	})

	// Test connection
	if err := rdb.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("failed to connect to Redis cluster: %w", err)
	}
	defer rdb.Close()

	fmt.Printf("Connected to Redis cluster: %v\n", config.RedisAddrs)
	fmt.Printf("Key pattern: %s\n", config.KeyPattern)
	fmt.Printf("Dry run: %v\n", config.DryRun)
	fmt.Printf("Concurrency: %d\n", config.Concurrency)
	fmt.Printf("Batch size: %d\n", config.BatchSize)
	fmt.Println("---")

	totalDeleted := 0
	startTime := time.Now()

	// Используем ClusterClient для сканирования и удаления
	var cursor uint64
	var keys []string

	for {
		var err error
		keys, cursor, err = rdb.Scan(ctx, cursor, config.KeyPattern, int64(config.BatchSize)).Result()
		if err != nil {
			return fmt.Errorf("scan failed: %w", err)
		}

		if len(keys) > 0 {
			if config.DryRun {
				fmt.Printf("Would delete %d keys: %v\n", len(keys), keys)
				totalDeleted += len(keys)
			} else {
				// Удаляем ключи через ClusterClient
				deleted, err := deleteKeysCluster(ctx, rdb, keys)
				if err != nil {
					return fmt.Errorf("delete failed: %w", err)
				}
				fmt.Printf("Deleted %d keys\n", deleted)
				totalDeleted += deleted
			}
		}

		if cursor == 0 {
			break
		}
	}

	duration := time.Since(startTime)

	fmt.Println("---")
	if config.DryRun {
		fmt.Printf("Dry run completed. Found %d matching keys\n", totalDeleted)
	} else {
		fmt.Printf("Deleted %d keys in %v\n", totalDeleted, duration)
	}

	return nil
}

func deleteKeysCluster(ctx context.Context, rdb *redis.ClusterClient, keys []string) (int, error) {
	// Используем pipeline для пакетного удаления через ClusterClient
	pipe := rdb.Pipeline()

	for _, key := range keys {
		pipe.Del(ctx, key)
	}

	results, err := pipe.Exec(ctx)
	if err != nil {
		return 0, fmt.Errorf("pipeline execution failed: %w", err)
	}

	deleted := 0
	for _, result := range results {
		if err := result.Err(); err != nil && err != redis.Nil {
			return deleted, fmt.Errorf("delete operation failed: %w", err)
		}
		deleted++
	}

	return deleted, nil
}

// Альтернативный вариант: удаление ключей по одному (медленнее, но проще)
func deleteKeysClusterOneByOne(ctx context.Context, rdb *redis.ClusterClient, keys []string) (int, error) {
	deleted := 0
	for _, key := range keys {
		err := rdb.Del(ctx, key).Err()
		if err != nil && err != redis.Nil {
			return deleted, fmt.Errorf("failed to delete key %s: %w", key, err)
		}
		deleted++
	}
	return deleted, nil
}
