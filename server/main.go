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

// Helper to handle scanning and consistent error reporting
func (s *bookCatalogServer) executeQuery(ctx context.Context, query string, args ...interface{}) (*pb.SearchBooksResponse, error) {
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "database error: %v", err)
	}
	defer rows.Close()


	var books []*pb.Book
	for rows.Next() {
		var b pb.Book
		// Scan order must match the SELECT order exactly
		if err := rows.Scan(&b.Id, &b.Title, &b.Author, &b.Isbn, &b.Price, &b.Stock, &b.PublishedYear); err != nil {
			return nil, status.Errorf(codes.Internal, "scan error: %v", err)
		}
		books = append(books, &b)
	}
	return &pb.SearchBooksResponse{Books: books, Count: int32(len(books))}, nil
}

// --- Task 4: SearchBooks (Case-Insensitive) ---
func (s *bookCatalogServer) SearchBooks(ctx context.Context, req *pb.SearchBooksRequest) (*pb.SearchBooksResponse, error) {

	if req.Query == "" {
		return nil, status.Error(codes.InvalidArgument, "search query required")
	}


	searchPattern := "%" + req.Query + "%"
	var query string
	var args []interface{}

	switch req.Field {
	case "title":
		query = "SELECT id, title, author, isbn, price, stock, published_year FROM books WHERE title LIKE ? COLLATE NOCASE"
		args = []interface{}{searchPattern}
	case "author":
		query = "SELECT id, title, author, isbn, price, stock, published_year FROM books WHERE author LIKE ? COLLATE NOCASE"
		args = []interface{}{searchPattern}
	case "isbn":
		query = "SELECT id, title, author, isbn, price, stock, published_year FROM books WHERE isbn = ?"
		args = []interface{}{req.Query}
	default:
		query = "SELECT id, title, author, isbn, price, stock, published_year FROM books WHERE (title LIKE ? OR author LIKE ? OR isbn LIKE ?) COLLATE NOCASE"
		args = []interface{}{searchPattern, searchPattern, searchPattern}
	}

	return s.executeQuery(ctx, query, args...)
}

// --- Task 4: FilterBooks ---
func (s *bookCatalogServer) FilterBooks(ctx context.Context, req *pb.FilterBooksRequest) (*pb.FilterBooksResponse, error) {
	if req.MinPrice < 0 || req.MaxPrice < 0 {
		return nil, status.Error(codes.InvalidArgument, "price cannot be negative")
	}
	if req.MaxPrice > 0 && req.MinPrice > req.MaxPrice {
		return nil, status.Error(codes.InvalidArgument, "min_price cannot be greater than max_price")
	}

	
	query := "SELECT id, title, author, isbn, price, stock, published_year FROM books WHERE 1=1"
	var args []interface{}


	if req.MinPrice > 0 {
		query += " AND price >= ?"; args = append(args, req.MinPrice)
	}
	if req.MaxPrice > 0 {
		query += " AND price <= ?"; args = append(args, req.MaxPrice)
	}
	
	if req.MinYear > 0 {
		query += " AND published_year >= ?"; args = append(args, req.MinYear)
	}
	if req.MaxYear > 0 {
		query += " AND published_year <= ?"; args = append(args, req.MaxYear)
	}

	
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "filter failed: %v", err)
	}
	defer rows.Close()


	var books []*pb.Book
	for rows.Next() {
		var b pb.Book
		rows.Scan(&b.Id, &b.Title, &b.Author, &b.Isbn, &b.Price, &b.Stock, &b.PublishedYear)
		books = append(books, &b)
	}
	return &pb.FilterBooksResponse{Books: books, Count: int32(len(books))}, nil
}

// --- Task 4: GetStats (Cleaned) ---
func (s *bookCatalogServer) GetStats(ctx context.Context, req *pb.GetStatsRequest) (*pb.
GetStatsResponse, error) {
	var stats pb.GetStatsResponse
	query := `SELECT 
                COUNT(*), 
                IFNULL(AVG(price), 0), 
                IFNULL(SUM(stock), 0), 
                IFNULL(MIN(published_year), 0), 
                IFNULL(MAX(published_year), 0) 
              FROM books WHERE published_year > 0`
	
	err := s.db.QueryRowContext(ctx, query).Scan(
		&stats.TotalBooks, &stats.AveragePrice, &stats.TotalStock, &stats.EarliestYear, &stats.LatestYear,
	)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "stats failed: %v", err)
	}
	return &stats, nil
}



func initDB() (*sql.DB, error) {
	db, err := sql.Open("sqlite3", "./books.db")
	if err != nil {
		return nil, err
	}
	// Note: Database has an author_id field but our proto doesn't use it yet 
	schema := `CREATE TABLE IF NOT EXISTS books (
		id INTEGER PRIMARY KEY AUTOINCREMENT, 
		title TEXT NOT NULL, 
		author TEXT NOT NULL, 
		isbn TEXT NOT NULL, 
		price REAL NOT NULL, 
		stock INTEGER NOT NULL, 
		published_year INTEGER NOT NULL
	);`
	if _, err := db.Exec(schema); err != nil {
		return nil, err
	}
	
	return db, nil
}

func main() {

	db, err := initDB()
	if err != nil {
		log.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	srv := grpc.NewServer()
	pb.RegisterBookCatalogServer(srv, &bookCatalogServer{db: db})

	log.Printf("📚 Server (Task 4) listening at %v", lis.Addr())
	if err := srv.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}