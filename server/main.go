package main

import (
	"context"
	"database/sql"
	"log"
	"net"

	pb "book-catalog-grpc/proto"

	_ "github.com/mattn/go-sqlite3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type bookCatalogServer struct {
	pb.UnimplementedBookCatalogServer
	db *sql.DB
}

func (s *bookCatalogServer) GetBook(ctx context.Context, req *pb.GetBookRequest) (*pb.GetBookResponse, error) {
	// TODO: Query book from database
	query := "SELECT id, title, author, isbn, price, stock, published_year FROM books WHERE id = ?"
	var b pb.Book
	err := s.db.QueryRowContext(ctx, query, req.Id).Scan(&b.Id, &b.Title, &b.Author, &b.Isbn, &b.Price, &b.Stock, &b.PublishedYear)
	// TODO: Handle not found case
	if err == sql.ErrNoRows {
		return nil, status.Errorf(codes.NotFound, "Book with ID %d not found", req.Id)
	}
	if err != nil {
		return nil, status.Errorf(codes.Internal, "Database error: %v", err)
	}
	// TODO: Return book
	return &pb.GetBookResponse{Book: &b}, nil
}

func (s *bookCatalogServer) CreateBook(ctx context.Context, req *pb.CreateBookRequest) (*pb.CreateBookResponse, error) {
	// TODO: Validate input
	if req.Title == "" || req.Author == "" || req.Isbn == "" {
		return nil, status.Errorf(codes.InvalidArgument, "Invalid input")
	}

	// TODO: Insert into database
	query := "INSERT INTO books (title, author, isbn, price, stock, published_year) VALUES (?, ?, ?, ?, ?, ?)"
	result, err := s.db.ExecContext(ctx, query, req.Title, req.Author, req.Isbn, req.Price, req.Stock, req.PublishedYear)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "Failed to insert book: %v", err)
	}

	// TODO: Return created book with ID
	id, _ := result.LastInsertId()
	return &pb.CreateBookResponse{
		Book: &pb.Book{
			Id:            int32(id),
			Title:         req.Title,
			Author:        req.Author,
			Isbn:          req.Isbn,
			Price:         req.Price,
			Stock:         req.Stock,
			PublishedYear: req.PublishedYear,
		},
	}, nil
}

func (s *bookCatalogServer) UpdateBook(ctx context.Context, req *pb.UpdateBookRequest) (*pb.UpdateBookResponse, error) {
	// TODO: Validate input
	if req.Id == 0 || req.Title == "" || req.Author == "" || req.Isbn == "" {
		return nil, status.Errorf(codes.InvalidArgument, "Invalid input")
	}

	// TODO: Update in database
	query := "UPDATE books SET title = ?, author = ?, isbn = ?, price = ?, stock = ?, published_year = ? WHERE id = ?"
	result, err := s.db.ExecContext(ctx, query, req.Title, req.Author, req.Isbn, req.Price, req.Stock, req.PublishedYear, req.Id)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "Failed to update book: %v", err)
	}
	// TODO: Check if exists (RowsAffected)
	rows, _ := result.RowsAffected()
	if rows == 0 {
		return nil, status.Errorf(codes.NotFound, "Book ID %d not found", req.Id)
	}

	// TODO: Return updated book
	return &pb.UpdateBookResponse{
		Book: &pb.Book{
			Id:            req.Id,
			Title:         req.Title,
			Author:        req.Author,
			Isbn:          req.Isbn,
			Price:         req.Price,
			Stock:         req.Stock,
			PublishedYear: req.PublishedYear,
		},
	}, nil
}

func (s *bookCatalogServer) DeleteBook(ctx context.Context, req *pb.DeleteBookRequest) (*pb.DeleteBookResponse, error) {
	// TODO: Delete from database
	query := "DELETE FROM books where id = ?"
	result, err := s.db.ExecContext(ctx, query, req.Id)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "Failed to delete book: %v", err)
	}

	// TODO: Check if exists
	rows, _ := result.RowsAffected()
	if rows == 0 {
		return nil, status.Errorf(codes.NotFound, "Book with ID %d not found", req.Id)
	}

	// TODO: Return success response
	return &pb.DeleteBookResponse{
		Success: true,
		Message: "Book deleted successfully!",
	}, nil
}

func (s *bookCatalogServer) ListBooks(ctx context.Context, req *pb.ListBooksRequest) (*pb.ListBooksResponse, error) {
	// TODO: Implement pagination
	if req.Page <= 0 {
		req.Page = 1
	}
	if req.PageSize <= 0 {
		req.PageSize = 10
	}

	// TODO: Get total count
	var total int64
	err := s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM books").Scan(&total)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "Failed to get count: %v", err)
	}

	// TODO: Query books with LIMIT/OFFSET
	offset := (req.Page - 1) * req.PageSize
	query := "SELECT id, title, author, isbn, price, stock, published_year FROM books LIMIT ? OFFSET ?"
	rows, err := s.db.QueryContext(ctx, query, req.PageSize, offset)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "Failed to list books: %v", err)
	}
	defer rows.Close()

	// TODO: Return response
	var books []*pb.Book
	for rows.Next() {
		var b pb.Book
		rows.Scan(&b.Id, &b.Title, &b.Author, &b.Isbn, &b.Price, &b.Stock, &b.PublishedYear)
		books = append(books, &b)
	}
	return &pb.ListBooksResponse{
		Books: books,
		Total: int32(total),
	}, nil
}

func initDB() (*sql.DB, error) {
	db, err := sql.Open("sqlite3", "./books.db")
	if err != nil {
		return nil, err
	}

	// TODO: Create books table
	schema := `
		CREATE TABLE IF NOT EXISTS books (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			title TEXT NOT NULL,
			author TEXT NOT NULL,
			isbn TEXT NOT NULL,
			price REAL NOT NULL,
			stock INTEGER NOT NULL,
			published_year INTEGER NOT NULL
		)
	`
	if _, err = db.Exec(schema); err != nil {
		return nil, err
	}

	// TODO: Seed sample data (5+ books)
	var count int
	db.QueryRow("SELECT COUNT(*) FROM books").Scan(&count)
	if count == 0 {
		books := []struct {
			title, author, isbn string
			price               float32
			stock, year         int
		}{
			{"The Go Programming Language", "Alan Donovan", "978-0134190440", 39.99, 10, 2015},
			{"Clean Code", "Robert Martin", "978-0132350884", 42.50, 5, 2008},
			{"The Pragmatic Programmer", "Andrew Hunt", "978-0135957059", 45.00, 8, 1999},
			{"Refactoring", "Martin Fowler", "978-0134757599", 44.99, 12, 2018},
			{"Designing Data-Intensive Applications", "Martin Kleppmann", "978-1449373320", 48.00, 15, 2017},
		}
		for _, b := range books {
			db.Exec("INSERT INTO books (title, author, isbn, price, stock, published_year) VALUES (?, ?, ?, ?, ?, ?)",
				b.title, b.author, b.isbn, b.price, b.stock, b.year)
		}
	}
	return db, nil
}

func main() {
	// TODO: Initialize database
	db, err := initDB()
	if err != nil {
		log.Fatal(err)
	}

	// TODO: Create listener
	listener, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	// TODO: Create gRPC server
	grpcServer := grpc.NewServer()

	// TODO: Register service
	pb.RegisterBookCatalogServer(grpcServer, &bookCatalogServer{db: db})

	// TODO: Start serving
	log.Printf("Server is listening at %v", listener.Addr())
	if err := grpcServer.Serve(listener); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}
