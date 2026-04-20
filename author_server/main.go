package main

import (
	"context"
	"database/sql"
	"log"
	"net"

	pb "book-catalog-grpc/proto" // Keep your current working proto path
	_ "github.com/mattn/go-sqlite3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type authorCatalogServer struct {
	pb.UnimplementedAuthorCatalogServer
	db         *sql.DB
	bookClient pb.BookCatalogClient // Client to Book service
}

// newServer initializes the server with the database and the gRPC client for the Book service
func newServer(db *sql.DB, bookClient pb.BookCatalogClient) *authorCatalogServer {
	return &authorCatalogServer{
		db:         db,
		bookClient: bookClient,
	}
}

// Task 5: The "Bridge" method (Service-to-Service Communication)
func (s *authorCatalogServer) GetAuthorBooks(ctx context.Context, req *pb.GetAuthorBooksRequest) (*pb.GetAuthorBooksResponse, error) {
	log.Printf("GetAuthorBooks: author_id=%d", req.AuthorId)

	// 1. Fetch Author from local authors.db
	var author pb.Author
	err := s.db.QueryRowContext(ctx,
		"SELECT id, name, bio, birth_year, country FROM authors WHERE id = ?",
		req.AuthorId,
	).Scan(&author.Id, &author.Name, &author.Bio, &author.BirthYear, &author.Country)

	if err == sql.ErrNoRows {
		return nil, status.Errorf(codes.NotFound, "author not found: id=%d", req.AuthorId)
	}

	// 2. INTER-SERVICE CALL: Reaching out to Book Service on 50051
	bookResp, err := s.bookClient.GetBooksByAuthor(ctx, &pb.GetBooksByAuthorRequest{
		AuthorId: author.Id,
	})
	
	if err != nil {
		log.Printf("Warning: Could not fetch books from Book Service: %v", err)
		// Return author even if book service is down (graceful degradation)
		return &pb.GetAuthorBooksResponse{Author: &author, Books: nil, BookCount: 0}, nil
	}

	// 3. Map the books from the Book Service to the Author Service's BookSummary
	var bookSummaries []*pb.BookSummary
	for _, book := range bookResp.Books {
		bookSummaries = append(bookSummaries, &pb.BookSummary{
			Id:            book.Id,
			Title:         book.Title,
			Price:         book.Price,
			PublishedYear: book.PublishedYear,
		})
	}

	return &pb.GetAuthorBooksResponse{
		Author:    &author,
		Books:     bookSummaries,
		BookCount: int32(len(bookSummaries)),
	}, nil
}

// CRUD: CreateAuthor
func (s *authorCatalogServer) CreateAuthor(ctx context.Context, req *pb.CreateAuthorRequest) (*pb.CreateAuthorResponse, error) {
	res, err := s.db.ExecContext(ctx, "INSERT INTO authors (name, bio, birth_year, country) VALUES (?, ?, ?, ?)",
		req.Name, req.Bio, req.BirthYear, req.Country)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "DB error: %v", err)
	}
	id, _ := res.LastInsertId()
	return &pb.CreateAuthorResponse{
		Author: &pb.Author{Id: int32(id), Name: req.Name, Bio: req.Bio, BirthYear: req.BirthYear, Country: req.Country},
	}, nil
}

// CRUD: GetAuthor
func (s *authorCatalogServer) GetAuthor(ctx context.Context, req *pb.GetAuthorRequest) (*pb.GetAuthorResponse, error) {
	var a pb.Author
	err := s.db.QueryRowContext(ctx, "SELECT id, name, bio, birth_year, country FROM authors WHERE id = ?", req.Id).
		Scan(&a.Id, &a.Name, &a.Bio, &a.BirthYear, &a.Country)
	
	if err == sql.ErrNoRows {
		return nil, status.Error(codes.NotFound, "Author not found")
	}
	return &pb.GetAuthorResponse{Author: &a}, nil
}

// CRUD: ListAuthors with pagination
func (s *authorCatalogServer) ListAuthors(ctx context.Context, req *pb.ListAuthorsRequest) (*pb.ListAuthorsResponse, error) {
	limit := req.PageSize
	if limit <= 0 { limit = 10 }
	offset := (req.Page - 1) * limit

	rows, err := s.db.QueryContext(ctx, "SELECT id, name, bio, birth_year, country FROM authors LIMIT ? OFFSET ?", limit, offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var authors []*pb.Author
	for rows.Next() {
		var a pb.Author
		rows.Scan(&a.Id, &a.Name, &a.Bio, &a.BirthYear, &a.Country)
		authors = append(authors, &a)
	}

	// Count total for response
	var total int32
	s.db.QueryRow("SELECT COUNT(*) FROM authors").Scan(&total)

	return &pb.ListAuthorsResponse{Authors: authors, Total: total}, nil
}

// Helper to connect to the other service
func connectToBookService() (pb.BookCatalogClient, error) {
	conn, err := grpc.Dial("localhost:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	return pb.NewBookCatalogClient(conn), nil
}

func main() {
	// 1. Initialize SQLite Database
	db, err := sql.Open("sqlite3", "./authors.db")
	if err != nil {
		log.Fatalf("Failed to open DB: %v", err)
	}
	db.Exec(`CREATE TABLE IF NOT EXISTS authors (
		id INTEGER PRIMARY KEY AUTOINCREMENT, 
		name TEXT, 
		bio TEXT, 
		birth_year INTEGER, 
		country TEXT
	)`)

	// 2. Connect to the Book Service (Port 50051)
	bookClient, err := connectToBookService()
	if err != nil {
		log.Fatalf("Failed to connect to Book service: %v", err)
	}

	
	// 3. Start the Author Server (Port 50052)
	lis, err := net.Listen("tcp", ":50052")
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}
	
	grpcServer := grpc.NewServer()
	
	// Register the service using the constructor
	pb.RegisterAuthorCatalogServer(grpcServer, newServer(db, bookClient))

	log.Println("🚀 Author Catalog gRPC server listening on :50052")
	log.Println("📚 Connected to Book Catalog service on :50051")
	
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
