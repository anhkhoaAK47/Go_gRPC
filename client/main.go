package main

import (
	"context"
	
	"fmt"
	"log"
	"time"

	pb "book-catalog-grpc/proto"
	
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

)

func main() {
	conn, err := grpc.Dial("localhost:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	
	client := pb.NewBookCatalogClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	// === Test 1: Search by Title ===
	fmt.Println("=== Test 1: Search by Title ===")
	fmt.Println("Searching for \"go\"...")
	res1, _ := client.SearchBooks(ctx, &pb.SearchBooksRequest{Query: "go", Field: "title"})
	fmt.Printf("Found %d books:\n", res1.Count)
	for _, b := range res1.Books {
		fmt.Printf("- %s\n", b.Title)
	}

	// === Test 2: Search by Author ===
	fmt.Println("\n=== Test 2: Search by Author ===")
	fmt.Println("Searching for \"Martin\"...")
	res2, _ := client.SearchBooks(ctx, &pb.SearchBooksRequest{Query: "Martin", Field: "author"})
	fmt.Printf("Found %d books:\n", res2.Count)
	for _, b := range res2.Books {
		fmt.Printf("- %s by %s\n", b.Title, b.Author)
	}

	// === Test 3: Filter by Price ===
	fmt.Println("\n=== Test 3: Filter by Price ===")
	fmt.Println("Books between $20 and $45:")
	res3, _ := client.FilterBooks(ctx, &pb.FilterBooksRequest{MinPrice: 20, MaxPrice: 45})
	fmt.Printf("Found %d books\n", res3.Count)

	// === Test 4: Filter by Year ===
	fmt.Println("\n=== Test 4: Filter by Year ===")
	fmt.Println("Books published after 2010:")
	res4, _ := client.FilterBooks(ctx, &pb.FilterBooksRequest{MinYear: 2010})
	fmt.Printf("Found %d books\n", res4.Count)

	// === Test 5: Get Statistics ===
	fmt.Println("\n=== Test 5: Get Statistics ===")
	stats, _ := client.GetStats(ctx, &pb.GetStatsRequest{})
	fmt.Printf("Total books: %d\n", stats.TotalBooks)
	fmt.Printf("Average price: $%.2f\n", stats.AveragePrice)
	fmt.Printf("Total stock: %d\n", stats.TotalStock)
	fmt.Printf("Year range: %d - %d\n", stats.EarliestYear, stats.LatestYear)

	// === Test 6: Error Cases ===
	fmt.Println("\n=== Test 6: Error Cases ===")
	_, errA := client.SearchBooks(ctx, &pb.SearchBooksRequest{Query: ""})
	fmt.Printf("Empty search query: Error: %v\n", grpc.ErrorDesc(errA))

	_, errB := client.FilterBooks(ctx, &pb.FilterBooksRequest{MinPrice: 100, MaxPrice: 50})
	fmt.Printf("Invalid price range: Error: %v\n", grpc.ErrorDesc(errB))
}
