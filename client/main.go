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
	// TODO: Connect to server
	conn, err := grpc.Dial("localhost:50051",
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	client := pb.NewBookCatalogClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	// List all books
	fmt.Println("=== Test 1: List ALL books ===")
	resp, err := client.ListBooks(ctx, &pb.ListBooksRequest{
		Page:     1,
		PageSize: 10,
	})

	if err != nil {
		log.Fatalf("Failed to list all books: %v", err)
	}

	fmt.Printf("Total Books: %d\n", resp.Total)
	for _, b := range resp.Books {
		fmt.Printf("%d. %s by %s - $%.2f\n", b.Id, b.Title, b.Author, b.Price)
	}

	// Get book
	fmt.Println("\n=== Test 2: Get Book ===")
	getResp, err := client.GetBook(ctx, &pb.GetBookRequest{Id: 1})
	if err != nil {
		log.Fatalf("Failed to get book: %v", err)
	}

	fmt.Printf("Book ID: %d\nTitle: %s\nAuthor: %s\nPrice: %.2f\n", getResp.Book.Id, getResp.Book.Title, getResp.Book.Author, getResp.Book.Price)

	// Create book
	fmt.Println("\n=== Test 3: Create Book ===")
	createResp, err := client.CreateBook(ctx, &pb.CreateBookRequest{
		Title:         "Learning Go",
		Author:        "Jon Bodner",
		Isbn:          "978-1492077213",
		Price:         29.99,
		Stock:         7,
		PublishedYear: 2021,
	})
	if err != nil {
		log.Fatalf("Failed to create book: %v", err)
	}

	fmt.Printf("Created Book ID: %d\nTitle: %s\n", createResp.Book.Id, createResp.Book.Title)

	// Update book
	fmt.Println("\n=== Test 4: Update Book ===")
	updateResp, err := client.UpdateBook(ctx, &pb.UpdateBookRequest{
		Id:            1,
		Title:         "The Go Programming Language (2nd Edition)",
		Author:        "Alan Donovan",
		Isbn:          "978-0134190440",
		Price:         35.99,
		Stock:         10,
		PublishedYear: 2016,
	})
	if err != nil {
		log.Fatalf("Failed to update book: %v", err)
	}

	fmt.Printf("Updated book: %s\nNew price: $%.2f\n", updateResp.Book.Title, updateResp.Book.Price)

	// Delete book
	fmt.Println("\n=== Test 5: Delete Book ===")
	deleteResp, err := client.DeleteBook(ctx, &pb.DeleteBookRequest{
		Id: 1,
	})
	if err != nil {
		log.Fatalf("Failed to delete book: %v", err)
	}

	fmt.Println("Book deleted successfully:", deleteResp.Success)

	// Pagination
	fmt.Println("\n=== Test 6: Pagination ===")
	paginatedResp1, err := client.ListBooks(ctx, &pb.ListBooksRequest{
		Page:     1,
		PageSize: 3,
	})
	if err != nil {
		log.Fatalf("Failed to list books with pagination: %v", err)
	}
	fmt.Printf("Page 1: %d books\n", len(paginatedResp1.Books))

	paginatedResp2, err := client.ListBooks(ctx, &pb.ListBooksRequest{
		Page:     2,
		PageSize: 2,
	})
	if err != nil {
		log.Fatalf("Failed to list books with pagination: %v", err)
	}
	fmt.Printf("Page 2: %d books\n", len(paginatedResp2.Books))

}
