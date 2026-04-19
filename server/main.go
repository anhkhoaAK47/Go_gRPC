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

// --- Task 5: New Method for Inter-Service Communication ---

func (s *bookCatalogServer) GetBooksByAuthor(ctx context.Context, req *pb.GetBooksByAuthorRequest) (*pb.GetBooksByAuthorResponse, error) {
	log.Printf("GetBooksByAuthor called for Author ID: %d", req.AuthorId)

	// Query books where author_id matches
	query := "SELECT id, title, author, isbn, price, stock, published_year, author_id FROM books WHERE author_id = ?"
	rows, err := s.db.QueryContext(ctx, query, req.AuthorId)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to query books by author: %v", err)
	}
	defer rows.Close()

	var books []*pb.Book
	for rows.Next() {
		var b pb.Book
		err := rows.Scan(&b.Id, &b.Title, &b.Author, &b.Isbn, &b.Price, &b.Stock, &b.PublishedYear, &b.AuthorId)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "failed to scan book: %v", err)
		}
		books = append(books, &b)
	}

	return &pb.GetBooksByAuthorResponse{Books: books}, nil
}

func (s *bookCatalogServer) GetBook(ctx context.Context, req *pb.GetBookRequest) (*pb.GetBookResponse, error) {
	query := "SELECT id, title, author, isbn, price, stock, published_year, author_id FROM books WHERE id = ?"
	var b pb.Book
	err := s.db.QueryRowContext(ctx, query, req.Id).Scan(&b.Id, &b.Title, &b.Author, &b.Isbn, &b.Price, &b.Stock, &b.PublishedYear, &b.AuthorId)
	
	if err == sql.ErrNoRows {
		return nil, status.Errorf(codes.NotFound, "Book with ID %d not found", req.Id)
	}
	if err != nil {
		return nil, status.Errorf(codes.Internal, "Database error: %v", err)
	}
	
	return &pb.GetBookResponse{Book: &b}, nil
}

func (s *bookCatalogServer) CreateBook(ctx context.Context, req *pb.CreateBookRequest) (*pb.CreateBookResponse, error) {
	
	if req.Title == "" || req.Author == "" || req.Isbn == "" {
		return nil, status.Errorf(codes.InvalidArgument, "title, author, and isbn are required")
	}

	// Task 5: Added author_id to the INSERT statement
	query := "INSERT INTO books (title, author, isbn, price, stock, published_year, author_id) VALUES (?, ?, ?, ?, ?, ?, ?)"
	result, err := s.db.ExecContext(ctx, query, req.Title, req.Author, req.Isbn, req.Price, req.Stock, req.PublishedYear, req.AuthorId)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to insert book: %v", err)
	}

	
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
			AuthorId:      req.AuthorId,
		},
	}, nil
}

func (s *bookCatalogServer) UpdateBook(ctx context.Context, req *pb.UpdateBookRequest) (*pb.UpdateBookResponse, error) {
	query := "UPDATE books SET title = ?, author = ?, isbn = ?, price = ?, stock = ?, published_year = ?, author_id = ? WHERE id = ?"
	result, err := s.db.ExecContext(ctx, query, req.Title, req.Author, req.Isbn, req.Price, req.Stock, req.PublishedYear, req.AuthorId, req.Id)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to update book: %v", err)
	}
	
	rows, _ := result.RowsAffected()
	if rows == 0 {
		return nil, status.Errorf(codes.NotFound, "Book ID %d not found", req.Id)
	}


	return &pb.UpdateBookResponse{
		Book: &pb.Book{
			Id:            req.Id,
			Title:         req.Title,
			Author:        req.Author,
			Isbn:          req.Isbn,
			Price:         req.Price,
			Stock:         req.Stock,
			PublishedYear: req.PublishedYear,
			AuthorId:      req.AuthorId,
		},
	}, nil
}

func (s *bookCatalogServer) DeleteBook(ctx context.Context, req *pb.DeleteBookRequest) (*pb.DeleteBookResponse, error) {
	result, err := s.db.ExecContext(ctx, "DELETE FROM books WHERE id = ?", req.Id)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to delete book: %v", err)
	}


	rows, _ := result.RowsAffected()
	if rows == 0 {
		return nil, status.Errorf(codes.NotFound, "Book with ID %d not found", req.Id)
	}

	return &pb.DeleteBookResponse{Success: true, Message: "Book deleted successfully"}, nil
}

func (s *bookCatalogServer) ListBooks(ctx context.Context, req *pb.ListBooksRequest) (*pb.ListBooksResponse, error) {
	if req.Page <= 0 { req.Page = 1 }
	if req.PageSize <= 0 { req.PageSize = 10 }

	var total int32
	s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM books").Scan(&total)

	offset := (req.Page - 1) * req.PageSize
	query := "SELECT id, title, author, isbn, price, stock, published_year, author_id FROM books LIMIT ? OFFSET ?"
	rows, err := s.db.QueryContext(ctx, query, req.PageSize, offset)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to list books: %v", err)
	}
	defer rows.Close()


	var books []*pb.Book
	for rows.Next() {
		var b pb.Book
		rows.Scan(&b.Id, &b.Title, &b.Author, &b.Isbn, &b.Price, &b.Stock, &b.PublishedYear, &b.AuthorId)
		books = append(books, &b)
	}
	return &pb.ListBooksResponse{Books: books, Total: total, Page: req.Page, PageSize: req.PageSize}, nil
}

// ... Task 4 methods (SearchBooks, FilterBooks, GetStats) remain the same ...

// --- Database Initialization ---

func initDB() (*sql.DB, error) {
	db, err := sql.Open("sqlite3", "./books.db")
	if err != nil {
		return nil, err
	}

	// Task 5: Added author_id column to the schema 
	schema := `
		CREATE TABLE IF NOT EXISTS books (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			title TEXT NOT NULL,
			author TEXT NOT NULL,
			isbn TEXT NOT NULL,
			price REAL NOT NULL,
			stock INTEGER NOT NULL,
			published_year INTEGER NOT NULL,
			author_id INTEGER DEFAULT 0
		);`
	
	if _, err = db.Exec(schema); err != nil {
		return nil, err
	}

	// Logic to add author_id column if it doesn't exist (for existing databases)
	_, _ = db.Exec("ALTER TABLE books ADD COLUMN author_id INTEGER DEFAULT 0")

	return db, nil
}

func main() {
	
	db, err := initDB()
	if err != nil {
		log.Fatalf("Failed to initialize database: %v", err)
	}

	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	s := grpc.NewServer()
	pb.RegisterBookCatalogServer(s, &bookCatalogServer{db: db})

	log.Printf("Book Server listening at %v", lis.Addr())
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
