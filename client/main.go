package main

import (
	"context"
	"fmt"
	"log"

	
	pb "book-catalog-grpc/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	// 1. Connect to both services
	bConn, _ := grpc.Dial("localhost:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
	aConn, _ := grpc.Dial("localhost:50052", grpc.WithTransportCredentials(insecure.NewCredentials()))
	defer bConn.Close()
	defer aConn.Close()

	bookClient := pb.NewBookCatalogClient(bConn)
	authorClient := pb.NewAuthorCatalogClient(aConn)
	ctx := context.Background()

	fmt.Println("=== Microservice Demo ===")

	// 2. Create Author (Request goes to 50052)
	fmt.Println("\n1. Creating author...")
	aResp, err := authorClient.CreateAuthor(ctx, &pb.CreateAuthorRequest{
		Name:      "Martin Fowler",
		Bio:       "Software expert",
		BirthYear: 1963,
		Country:   "UK",
	})
	if err != nil {
		log.Fatalf("Fail: %v", err)
	}
	fmt.Printf("✓ Created author: %s (ID: %d)\n", aResp.Author.Name, aResp.Author.Id)

	// 3. Create Books for that Author (Requests go to 50051)
	fmt.Println("\n2. Creating books for author...")

	// First Book
	_, err = bookClient.CreateBook(ctx, &pb.CreateBookRequest{
		Title:         "Refactoring",
		AuthorId:      aResp.Author.Id,
		Isbn:          "978-0134",
		Price:         49.99,
		PublishedYear: 2018, // Added PublishedYear
	})
	if err == nil {
		fmt.Println("✓ Created book: Refactoring")
	}

	// Second Book
	_, err = bookClient.CreateBook(ctx, &pb.CreateBookRequest{
		Title:         "Patterns of Enterprise Application Architecture",
		AuthorId:      aResp.Author.Id,
		Isbn:          "978-0321",
		Price:         54.99,
		PublishedYear: 2002, // Added PublishedYear
	})
	if err == nil {
		fmt.Println("✓ Created book: Patterns of Enterprise Application Architecture")
	}

	// 4. Fetch integrated data (The Cross-Service Test)
	fmt.Println("\n3. Fetching author's books (cross-service call)...")
	res, err := authorClient.GetAuthorBooks(ctx, &pb.GetAuthorBooksRequest{AuthorId: aResp.Author.Id})
	if err != nil {
		log.Fatalf("Cross-service call failed: %v", err)
	}

	fmt.Printf("✓ Author: %s\n", res.Author.Name)
	fmt.Printf("✓ Books written: %d\n", res.BookCount)
	for i, b := range res.Books {
		fmt.Printf("  %d. %s (%d)\n", i+1, b.Title, b.PublishedYear)
	}

	fmt.Println("\n✓ Microservice demo completed!")
}
