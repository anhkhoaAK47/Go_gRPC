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

// Requirement: Create GetBooksByAuthor RPC method
func (s *bookCatalogServer) GetBooksByAuthor(ctx context.Context, req *pb.GetBooksByAuthorRequest) (*pb.GetBooksByAuthorResponse, error) {

	
	query := "SELECT id, title, author, isbn, price, stock, published_year, author_id FROM books WHERE author_id = ?"
	rows, err := s.db.QueryContext(ctx, query, req.AuthorId)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to query: %v", err)
	}
	defer rows.Close()

	var books []*pb.Book
	for rows.Next() {
		var b pb.Book
		rows.Scan(&b.Id, &b.Title, &b.Author, &b.Isbn, &b.Price, &b.Stock, &b.PublishedYear, &b.AuthorId)
		books = append(books, &b)
	}

	return &pb.GetBooksByAuthorResponse{Books: books}, nil
}

// Requirement: Add author_id field to Book management
func (s *bookCatalogServer) CreateBook(ctx context.Context, req *pb.CreateBookRequest) (*pb.CreateBookResponse, error) {

	query := "INSERT INTO books (title, author, isbn, price, stock, published_year, author_id) VALUES (?, ?, ?, ?, ?, ?, ?)"
	res, err := s.db.ExecContext(ctx, query, req.Title, req.Author, req.Isbn, req.Price, req.Stock, req.PublishedYear, req.AuthorId)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to insert: %v", err)
	}
	id, _ := res.LastInsertId()
	return &pb.CreateBookResponse{Book: &pb.Book{Id: int32(id), Title: req.Title, AuthorId: req.AuthorId}}, nil
}

// Task 4 logic preserved for stats requirement
func (s *bookCatalogServer) GetStats(ctx context.Context, req *pb.GetStatsRequest) (*pb.GetStatsResponse, error) {
	var stats pb.GetStatsResponse
	query := `SELECT COUNT(*), IFNULL(AVG(price), 0), IFNULL(SUM(stock), 0), 
              IFNULL(MIN(published_year), 0), IFNULL(MAX(published_year), 0) FROM books`
	err := s.db.QueryRowContext(ctx, query).Scan(
		&stats.TotalBooks, &stats.AveragePrice, &stats.TotalStock, &stats.EarliestYear, &stats.LatestYear,
	)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "stats failed: %v", err)
	}
	return &stats, nil
}



func initDB() (*sql.DB, error) {
	db, _ := sql.Open("sqlite3", "./books.db")
	db.Exec(`CREATE TABLE IF NOT EXISTS books (
		id INTEGER PRIMARY KEY AUTOINCREMENT, title TEXT, author TEXT, isbn TEXT, 
		price REAL, stock INTEGER, published_year INTEGER, author_id INTEGER DEFAULT 0
	);`)
	db.Exec("ALTER TABLE books ADD COLUMN author_id INTEGER DEFAULT 0") // Migration
	return db, nil
}

func main() {
	db, _ := initDB()
	lis, _ := net.Listen("tcp", ":50051")
	srv := grpc.NewServer()
	pb.RegisterBookCatalogServer(srv, &bookCatalogServer{db: db})
	log.Println("📚 Book Service running on :50051")
	srv.Serve(lis)
}