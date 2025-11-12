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
	MaxRetries  int
}

func main() {
	var addrs string
	var keyPattern string
	var dryRun bool
	var concurrency int
	var batchSize int
	var maxRetries int

	flag.StringVar(&addrs, "a", "localhost:6379", "Redis cluster addresses (comma-separated)")
	flag.StringVar(&keyPattern, "k", "", "Key pattern to match (required)")
	flag.BoolVar(&dryRun, "dry-run", false, "Dry run mode (only show keys, don't delete)")
	flag.IntVar(&concurrency, "c", 10, "Concurrency level for scanning")
	flag.IntVar(&batchSize, "b", 1000, "Batch size for deletion")
	flag.IntVar(&maxRetries, "r", 3, "Max retries for delete operations")
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
		MaxRetries:  maxRetries,
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
	fmt.Printf("Max retries: %d\n", config.MaxRetries)
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
	fmt.Printf("Total keys found: %d\n", totalKeys)

	if config.DryRun {
		if totalKeys > 0 {
			fmt.Printf("Keys that would be deleted %d: %v\n", totalKeys, allKeys[:10])
		}
	} else {
		// Удаляем все ключи через ClusterClient
		if totalKeys > 0 {
			deleted, err := deleteKeysCluster(ctx, rdb, allKeys, config.BatchSize, config.MaxRetries)
			if err != nil {
				return fmt.Errorf("failed to delete keys: %w", err)
			}
			fmt.Printf("Successfully deleted %d keys\n", deleted)

			// Проверяем, все ли ключи удалились
			if deleted != totalKeys {
				fmt.Printf("Warning: expected to delete %d keys, but only %d were deleted\n", totalKeys, deleted)
			}
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
	// Альтернативный подход: используем ClusterSlots
	clusterSlots, err := rdb.ClusterSlots(ctx).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to get cluster slots: %w", err)
	}

	masterAddrs := make(map[string]bool)

	for _, slot := range clusterSlots {
		if len(slot.Nodes) > 0 {
			// Первая нода в слоте - это мастер
			master := slot.Nodes[0]
			masterAddrs[master.Addr] = true
		}
	}

	var masters []string
	for addr := range masterAddrs {
		masters = append(masters, addr)
	}

	if len(masters) == 0 {
		// Fallback: use provided addresses
		fmt.Println("Warning: no master nodes found via ClusterSlots, using provided addresses")
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
		Addr:     masterAddr,
		PoolSize: config.Concurrency,
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
			return keys, fmt.Errorf("scan failed on %s: %w", masterAddr, err)
		}

		keys = append(keys, batchKeys...)

		cursor = nextCursor
		if cursor == 0 {
			break
		}
	}

	return keys, nil
}

func deleteKeysCluster(ctx context.Context, rdb *redis.ClusterClient, keys []string, batchSize int, maxRetries int) (int, error) {
	totalDeleted := 0

	// Удаляем ключи батчами
	for i := 0; i < len(keys); i += batchSize {
		end := i + batchSize
		if end > len(keys) {
			end = len(keys)
		}

		batch := keys[i:end]

		// Пытаемся удалить батч с повторными попытками
		deleted, err := deleteKeysOneByOne(ctx, rdb, batch, maxRetries)
		if err != nil {
			return totalDeleted, fmt.Errorf("failed to delete batch %d-%d: %w", i, end-1, err)
		}

		totalDeleted += deleted
		fmt.Printf("Deleted batch %d-%d: %d/%d keys total\n", i, end-1, totalDeleted, len(keys))
	}

	return totalDeleted, nil
}

func retryDeleteBatch(ctx context.Context, rdb *redis.ClusterClient, keys []string, maxRetries int) (int, error) {
	var err error
	var deleted int

	for attempt := 1; attempt <= maxRetries; attempt++ {
		deleted, err = deleteKeysBatch(ctx, rdb, keys)
		if err == nil {
			return deleted, nil
		}

		if attempt < maxRetries {
			fmt.Printf("Attempt %d/%d failed: %v. Retrying in 1 second...\n", attempt, maxRetries, err)
			time.Sleep(time.Second)
		}
	}

	return 0, fmt.Errorf("all %d attempts failed: %w", maxRetries, err)
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
			// Логируем ошибку, но продолжаем подсчет
			fmt.Printf("Warning: failed to delete key: %v\n", err)
			continue
		}
		deleted++
	}

	return deleted, nil
}

// Альтернативный метод: удаление по одному ключу (надежнее, но медленнее)
func deleteKeysOneByOne(ctx context.Context, rdb *redis.ClusterClient, keys []string, maxRetries int) (int, error) {
	deleted := 0

	for _, key := range keys {
		var err error

		for attempt := 1; attempt <= maxRetries; attempt++ {
			err = rdb.Del(ctx, key).Err()
			if err == nil || err == redis.Nil {
				deleted++
				break
			}

			if attempt < maxRetries {
				fmt.Printf("Key %s: attempt %d/%d failed: %v. Retrying...\n", key, attempt, maxRetries, err)
				time.Sleep(100 * time.Millisecond)
			}
		}

		if err != nil && err != redis.Nil {
			return deleted, fmt.Errorf("failed to delete key %s after %d attempts: %w", key, maxRetries, err)
		}
	}

	return deleted, nil
}
