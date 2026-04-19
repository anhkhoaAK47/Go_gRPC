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
	// 1. Connect to Book Service (Port 50051)
	bookConn, err := grpc.Dial("localhost:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect to Book Service: %v", err)
	}
	defer bookConn.Close()
	bookClient := pb.NewBookCatalogClient(bookConn)

	// 2. Connect to Author Service (Port 50052)
	authorConn, err := grpc.Dial("localhost:50052", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect to Author Service: %v", err)
	}
	defer authorConn.Close()
	authorClient := pb.NewAuthorCatalogClient(authorConn)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	// --- TASK 5 TEST: CROSS-SERVICE COMMUNICATION ---
	fmt.Println("=== Task 5: Testing Cross-Service Communication ===")

	// Step A: Create an Author in the Author Service
	fmt.Println("1. Creating Author...")
	aResp, err := authorClient.CreateAuthor(ctx, &pb.CreateAuthorRequest{
		Name:      "Martin Kleppmann",
		Bio:       "Distributed Systems Researcher",
		BirthYear: 1980,
		Country:   "Germany",
	})
	if err != nil {
		log.Fatalf("Failed to create author: %v", err)
	}
	authorID := aResp.Author.Id
	fmt.Printf("Created Author: %s (ID: %d)\n", aResp.Author.Name, authorID)

	// Step B: Create a Book in the Book Service linked to that Author ID
	fmt.Println("2. Creating Book linked to Author...")
	_, err = bookClient.CreateBook(ctx, &pb.CreateBookRequest{
		Title:         "Designing Data-Intensive Applications",
		Author:        "Martin Kleppmann",
		Isbn:          "978-1449373320",
		Price:         45.00,
		Stock:         20,
		PublishedYear: 2017,
		AuthorId:      authorID, // The link!
	})
	if err != nil {
		log.Fatalf("Failed to create book: %v", err)
	}

	// Step C: Call GetAuthorBooks (Author Service will call Book Service internally)
	fmt.Println("3. Fetching Author + Books (Aggregation)...")
	finalResp, err := authorClient.GetAuthorBooks(ctx, &pb.GetAuthorBooksRequest{AuthorId: authorID})
	if err != nil {
		log.Fatalf("Failed to get aggregated data: %v", err)
	}

	fmt.Printf("\nResult from Author Service:\n")
	fmt.Printf("Author: %s\n", finalResp.Author.Name)
	fmt.Printf("Books found in Book Service: %d\n", finalResp.BookCount)
	for _, b := range finalResp.Books {
		fmt.Printf("- %s ($%.2f)\n", b.Title, b.Price)
	}

	// --- Existing Book Service Tests (Test 1 & 2 only for brevity) ---
	fmt.Println("\n=== Original Book Service Tests ===")
	listResp, _ := bookClient.ListBooks(ctx, &pb.ListBooksRequest{Page: 1, PageSize: 5})
	fmt.Printf("Book List Count: %d\n", len(listResp.Books))
}
