package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
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

	// Create Redis cluster client для удаления
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

	// Получаем все мастер-узлы для сканирования
	masters, err := getMasterNodes(ctx, rdb)
	if err != nil {
		return fmt.Errorf("failed to get master nodes: %w", err)
	}

	fmt.Printf("Found %d master nodes:\n", len(masters))
	for _, master := range masters {
		fmt.Printf("  - %s\n", master)
	}
	fmt.Println("---")

	totalKeys := 0
	startTime := time.Now()

	// Сканируем все ключи со всех мастер-узлов
	allKeys, err := scanAllMasterNodes(ctx, masters, config)
	if err != nil {
		return fmt.Errorf("failed to scan keys: %w", err)
	}

	totalKeys = len(allKeys)

	if config.DryRun {
		fmt.Printf("Would delete %d keys\n", totalKeys)
		if totalKeys > 0 {
			fmt.Printf("Keys: %v\n", allKeys)
		}
	} else {
		// Удаляем все ключи через ClusterClient
		if totalKeys > 0 {
			deleted, err := deleteKeysCluster(ctx, rdb, allKeys, config.BatchSize)
			if err != nil {
				return fmt.Errorf("failed to delete keys: %w", err)
			}
			fmt.Printf("Deleted %d keys\n", deleted)
		} else {
			fmt.Println("No keys found to delete")
		}
	}

	duration := time.Since(startTime)

	fmt.Println("---")
	if config.DryRun {
		fmt.Printf("Dry run completed. Found %d matching keys\n", totalKeys)
	} else {
		fmt.Printf("Operation completed in %v. Total keys processed: %d\n", duration, totalKeys)
	}

	return nil
}

func getMasterNodes(ctx context.Context, rdb *redis.ClusterClient) ([]string, error) {
	// Используем встроенный метод для получения информации о кластере
	clusterInfo, err := rdb.ClusterNodes(ctx).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to get cluster nodes: %w", err)
	}

	var masters []string
	lines := strings.Split(clusterInfo, "\n")

	for _, line := range lines {
		if line == "" {
			continue
		}

		parts := strings.Fields(line)
		if len(parts) < 8 {
			continue
		}

		// Проверяем, что это мастер-узел (не реплика)
		flags := parts[2]
		if strings.Contains(flags, "master") && !strings.Contains(flags, "fail") {
			// Извлекаем адрес (формат: host:port@busport)
			addrPart := parts[1]
			addr := strings.Split(addrPart, "@")[0] // Убираем часть после @

			// Проверяем, что адрес не дублируется
			found := false
			for _, existing := range masters {
				if existing == addr {
					found = true
					break
				}
			}

			if !found {
				masters = append(masters, addr)
			}
		}
	}

	if len(masters) == 0 {
		// Fallback: use provided addresses
		return rdb.Options().Addrs, nil
	}

	return masters, nil
}

func scanAllMasterNodes(ctx context.Context, masters []string, config Config) ([]string, error) {
	var allKeys []string
	var mu sync.Mutex
	var wg sync.WaitGroup
	errors := make(chan error, len(masters))

	for _, masterAddr := range masters {
		wg.Add(1)
		go func(addr string) {
			defer wg.Done()

			keys, err := scanMasterNode(ctx, addr, config)
			if err != nil {
				errors <- fmt.Errorf("master %s: %w", addr, err)
				return
			}

			mu.Lock()
			allKeys = append(allKeys, keys...)
			mu.Unlock()

			fmt.Printf("Scanned %d keys from %s\n", len(keys), addr)
		}(masterAddr)
	}

	wg.Wait()
	close(errors)

	// Проверяем ошибки
	for err := range errors {
		return nil, err
	}

	return allKeys, nil
}

func scanMasterNode(ctx context.Context, masterAddr string, config Config) ([]string, error) {
	// Создаем клиент для конкретного мастер-узла
	client := redis.NewClient(&redis.Options{
		Addr: masterAddr,
	})
	defer client.Close()

	// Проверяем соединение
	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to master %s: %w", masterAddr, err)
	}

	var keys []string
	var cursor uint64

	for {
		// Сканируем ключи на этом узле
		batchKeys, nextCursor, err := client.Scan(ctx, cursor, config.KeyPattern, int64(config.BatchSize)).Result()
		if err != nil {
			return keys, fmt.Errorf("scan failed: %w", err)
		}

		keys = append(keys, batchKeys...)

		cursor = nextCursor
		if cursor == 0 {
			break
		}
	}

	return keys, nil
}

func deleteKeysCluster(ctx context.Context, rdb *redis.ClusterClient, keys []string, batchSize int) (int, error) {
	totalDeleted := 0

	// Удаляем ключи батчами
	for i := 0; i < len(keys); i += batchSize {
		end := i + batchSize
		if end > len(keys) {
			end = len(keys)
		}

		batch := keys[i:end]
		deleted, err := deleteKeysBatch(ctx, rdb, batch)
		if err != nil {
			return totalDeleted, fmt.Errorf("batch deletion failed: %w", err)
		}

		totalDeleted += deleted
		fmt.Printf("Deleted batch: %d/%d keys\n", totalDeleted, len(keys))
	}

	return totalDeleted, nil
}

func deleteKeysBatch(ctx context.Context, rdb *redis.ClusterClient, keys []string) (int, error) {
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
