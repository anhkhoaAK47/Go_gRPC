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

// Task 4: Advanced Search and Error Handling
func (s *bookCatalogServer) SearchBooks(ctx context.Context, req *pb.SearchBooksRequest) (*pb.SearchBooksResponse, error) {
	log.Printf("SearchBooks: query=%s, field=%s", req.Query, req.Field)

	// TODO: Validate input
	if req.Query == "" {
		return nil, status.Error(codes.InvalidArgument, "search query required")
	}

	// TODO: Build SQL query based on field
	var sqlQuery string
	var args []interface{}
	searchPattern := "%" + req.Query + "%"

	switch req.Field {
	case "title":
		// TODO: Search only in title
		sqlQuery = "SELECT id, title, author, isbn, price, stock, published_year FROM books WHERE title LIKE ?"
		args = append(args, searchPattern)
	case "author":
		// TODO: Search only in author
		sqlQuery = "SELECT id, title, author, isbn, price, stock, published_year FROM books WHERE author LIKE ?"
		args = append(args, searchPattern)
	case "isbn":
		// TODO: Search only in ISBN (exact match)
		sqlQuery = "SELECT id, title, author, isbn, price, stock, published_year FROM books WHERE isbn = ?"
		args = append(args, req.Query)
	case "all", "":
		// TODO: Search in all fields
		sqlQuery = "SELECT id, title, author, isbn, price, stock, published_year FROM books WHERE title LIKE ? OR author LIKE ? OR isbn LIKE ?"
		args = append(args, searchPattern, searchPattern, searchPattern)
	default:
		return nil, status.Errorf(codes.InvalidArgument, "invalid field: %s", req.Field)
	}

	// TODO: Execute query
	rows, err := s.db.QueryContext(ctx, sqlQuery, args...)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to search books: %v", err)
	}
	defer rows.Close()

	// TODO: Scan results
	var books []*pb.Book
	for rows.Next() {
		var b pb.Book
		err := rows.Scan(&b.Id, &b.Title, &b.Author, &b.Isbn, &b.Price, &b.Stock, &b.PublishedYear)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "failed to scan book: %v", err)
		}
		books = append(books, &b)
	}

	// TODO: Return response
	return &pb.SearchBooksResponse{
		Books: books,
		Count: int32(len(books)),
		Query: req.Query,
	}, nil
}

func (s *bookCatalogServer) FilterBooks(ctx context.Context, req *pb.FilterBooksRequest) (*pb.FilterBooksResponse, error) {
	log.Printf("FilterBooks: price[%.2f-%.2f], year[%d-%d]", req.MinPrice, req.MaxPrice, req.MinYear, req.MaxYear)

	// TODO: Validate ranges
	if req.MinPrice < 0 || req.MaxPrice < 0 {
		return nil, status.Error(codes.InvalidArgument, "price cannot be negative")
	}
	if req.MaxPrice > 0 && req.MinPrice > req.MaxPrice {
		return nil, status.Error(codes.InvalidArgument, "min_price cannot be greater than max_price")
	}

	// TODO: Build dynamic query
	query := "SELECT id, title, author, isbn, price, stock, published_year FROM books WHERE 1=1"
	var args []interface{}

	// TODO: Add price filters if provided
	if req.MinPrice > 0 {
		query += " AND price >= ?"
		args = append(args, req.MinPrice)
	}
	if req.MaxPrice > 0 {
		query += " AND price <= ?"
		args = append(args, req.MaxPrice)
	}

	// TODO: Add year filters if provided
	if req.MinYear > 0 {
		query += " AND published_year >= ?"
		args = append(args, req.MinYear)
	}
	if req.MaxYear > 0 {
		query += " AND published_year <= ?"
		args = append(args, req.MaxYear)
	}

	// TODO: Execute query
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to filter books: %v", err)
	}
	defer rows.Close()

	// TODO: Return results
	var books []*pb.Book
	for rows.Next() {
		var b pb.Book
		err := rows.Scan(&b.Id, &b.Title, &b.Author, &b.Isbn, &b.Price, &b.Stock, &b.PublishedYear)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "failed to scan book: %v", err)
		}
		books = append(books, &b)
	}

	return &pb.FilterBooksResponse{
		Books: books,
		Count: int32(len(books)),
	}, nil
}

func (s *bookCatalogServer) GetStats(ctx context.Context, req *pb.GetStatsRequest) (*pb.GetStatsResponse, error) {
	log.Println("GetStats called")

	var stats pb.GetStatsResponse

	// TODO: Get total books
	err := s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM books").Scan(&stats.TotalBooks)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to count books: %v", err)
	}

	// If no books, return empty stats
	if stats.TotalBooks == 0 {
		return &stats, nil
	}

	// TODO: Get average price
	err = s.db.QueryRowContext(ctx, "SELECT AVG(price) FROM books").Scan(&stats.AveragePrice)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to get average price: %v", err)
	}

	// TODO: Get total stock
	err = s.db.QueryRowContext(ctx, "SELECT SUM(stock) FROM books").Scan(&stats.TotalStock)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to get total stock: %v", err)
	}

	// TODO: Get earliest and latest year
	err = s.db.QueryRowContext(ctx, "SELECT MIN(published_year), MAX(published_year) FROM books").Scan(&stats.EarliestYear, &stats.LatestYear)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to get year range: %v", err)
	}

	return &stats, nil
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
