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

	// Get all master nodes
	masters, err := getMasterNodes(ctx, rdb)
	if err != nil {
		return fmt.Errorf("failed to get master nodes: %w", err)
	}

	fmt.Printf("Found %d master nodes:\n", len(masters))
	for _, master := range masters {
		fmt.Printf("  - %s\n", master)
	}
	fmt.Println("---")

	totalDeleted := 0
	startTime := time.Now()

	// Process each master node
	for _, masterAddr := range masters {
		deleted, err := processMasterNode(ctx, masterAddr, config)
		if err != nil {
			return fmt.Errorf("failed to process master node %s: %w", masterAddr, err)
		}
		totalDeleted += deleted
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

func getMasterNodes(ctx context.Context, rdb *redis.ClusterClient) ([]string, error) {
	// Get cluster info to find master nodes
	clusterInfo, err := rdb.ClusterNodes(ctx).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to get cluster nodes: %w", err)
	}

	var masters []string
	lines := strings.Split(clusterInfo, "\n")

	for _, line := range lines {
		if strings.Contains(line, "master") && !strings.Contains(line, "myself,master") {
			parts := strings.Fields(line)
			if len(parts) >= 2 {
				addr := strings.Split(parts[1], "@")[0] // Remove @busport part
				masters = append(masters, addr)
			}
		}
	}

	if len(masters) == 0 {
		// Fallback: if no masters found, use the provided addresses
		return rdb.Options().Addrs, nil
	}

	return masters, nil
}

func processMasterNode(ctx context.Context, masterAddr string, config Config) (int, error) {
	// Create client for specific master node
	client := redis.NewClient(&redis.Options{
		Addr: masterAddr,
	})
	defer client.Close()

	// Test connection to master
	if err := client.Ping(ctx).Err(); err != nil {
		return 0, fmt.Errorf("failed to connect to master %s: %w", masterAddr, err)
	}

	fmt.Printf("Processing master node: %s\n", masterAddr)

	var cursor uint64
	var keys []string
	totalDeleted := 0

	for {
		var err error
		keys, cursor, err = client.Scan(ctx, cursor, config.KeyPattern, int64(config.BatchSize)).Result()
		if err != nil {
			return totalDeleted, fmt.Errorf("scan failed: %w", err)
		}

		if len(keys) > 0 {
			if config.DryRun {
				fmt.Printf("  Would delete %d keys: %v\n", len(keys), keys)
				totalDeleted += len(keys)
			} else {
				// Delete keys in batches
				deleted, err := deleteKeys(ctx, client, keys)
				if err != nil {
					return totalDeleted, fmt.Errorf("delete failed: %w", err)
				}
				fmt.Printf("  Deleted %d keys from %s\n", deleted, masterAddr)
				totalDeleted += deleted
			}
		}

		if cursor == 0 {
			break
		}
	}

	return totalDeleted, nil
}

func deleteKeys(ctx context.Context, client *redis.Client, keys []string) (int, error) {
	// Use pipeline for batch deletion
	pipe := client.Pipeline()

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
