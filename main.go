package main

import (
	"fmt"
	"log"
	
	pb "book-catalog-grpc/proto"
	"google.golang.org/protobuf/proto"
)

func main() {
	// TODO: Create a Book
	book := &pb.Book{
		// Fill in fields
		Id: 1, 
		Title: "Random Book",
		Author: "Alan Donovan",
		Isbn: "978-0134190440",
		Price: 39.99,
		Stock: 15,
		PublishedYear: 2015,
	}
	
	fmt.Printf("Book: %v\n", book)
	
	// TODO: Create DetailedBook with category and tags
	detailedBook := &pb.DetailedBook{
		// Fill in fields
		Book: book,
		Description: "...",
		Category: pb.BookCategory_NONFICTION,
		Tags: []string{"programming", "go", "technical"},
		Rating: 4.5,
	}
	
	fmt.Printf("\nDetailed Book: %v\n", detailedBook)
	fmt.Printf("Category: %s\n", detailedBook.Category)
	fmt.Printf("Tags: %v\n", detailedBook.Tags)
	
	// TODO: Serialize to bytes
	data, err := proto.Marshal(book)
	if err != nil {
		log.Fatal(err)
	}
	
	fmt.Printf("\nSerialized size: %d bytes\n", len(data))
	
	// TODO: Deserialize from bytes
	newBook := &pb.Book{}
	err = proto.Unmarshal(data, newBook)
	if err != nil {
		log.Fatal(err)
	}
	
	fmt.Printf("Deserialized book: %v\n", newBook)
	
	// TODO: Create Author with multiple books
	author := &pb.Author{
		// Fill in fields with at least 2 books
		Name: "Robert C. Martin",
		Books: []*pb.Book{
			{
				Id: 1,
				Title: "Clean Code",
			},
			{
				Id: 2,
				Title: "Clean Architecture",
			},
		},
	}
	
	fmt.Printf("\nAuthor: %s\n", author.Name)
	fmt.Printf("Books written: %d\n", len(author.Books))
	for i, b := range author.Books {
		fmt.Printf("  %d. %s\n", i+1, b.Title)
	}
}