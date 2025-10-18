package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/aminovpavel/meshpipe-go/internal/diff"
)

func main() {
	var (
		oldPath = flag.String("old", "", "Path to baseline (Python) SQLite database")
		newPath = flag.String("new", "", "Path to Go-generated SQLite database")
		sample  = flag.Int("samples", 5, "Number of sample differences per table and side")
	)
	flag.Parse()

	if *oldPath == "" || *newPath == "" {
		flag.Usage()
		os.Exit(2)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	summary, err := diff.CompareSQLite(ctx, *oldPath, *newPath, diff.Options{
		SampleLimit: *sample,
	})
	if err != nil {
		log.Fatalf("meshpipe-diff: %v", err)
	}

	fmt.Println("=== packet_history ===")
	printTableDiff(summary.PacketHistory)
	fmt.Println()

	fmt.Println("=== node_info ===")
	printTableDiff(summary.NodeInfo)
}

func printTableDiff(td diff.TableDiff) {
	fmt.Printf("Only in first: %d rows\n", td.OnlyA)
	if len(td.SampleOnlyA) > 0 {
		fmt.Println("  Samples:")
		for _, s := range td.SampleOnlyA {
			fmt.Printf("    %s\n", s)
		}
	}
	fmt.Printf("Only in second: %d rows\n", td.OnlyB)
	if len(td.SampleOnlyB) > 0 {
		fmt.Println("  Samples:")
		for _, s := range td.SampleOnlyB {
			fmt.Printf("    %s\n", s)
		}
	}
}
