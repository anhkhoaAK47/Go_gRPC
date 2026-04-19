package main

import (
	"context"
	"database/sql"
	"log"
	"net"

	pb "book-catalog-grpc/proto" // Both services share this package now

	_ "github.com/mattn/go-sqlite3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type authorServer struct {
	pb.UnimplementedAuthorCatalogServer
	db         *sql.DB
	bookClient pb.BookCatalogClient // Client to talk to Book Service
}

// Task 5: The "Bridge" method (Service-to-Service Communication)
func (s *authorServer) GetAuthorBooks(ctx context.Context, req *pb.GetAuthorBooksRequest) (*pb.GetAuthorBooksResponse, error) {
	// 1. Fetch Author from local authors.db
	var author pb.Author
	err := s.db.QueryRowContext(ctx, "SELECT id, name, bio, birth_year, country FROM authors WHERE id = ?", req.AuthorId).
		Scan(&author.Id, &author.Name, &author.Bio, &author.BirthYear, &author.Country)
	
	if err == sql.ErrNoRows {
		return nil, status.Errorf(codes.NotFound, "Author with ID %d not found", req.AuthorId)
	}

	// 2. Call the Book Service via gRPC (Inter-service call)
	bookResp, err := s.bookClient.GetBooksByAuthor(ctx, &pb.GetBooksByAuthorRequest{AuthorId: author.Id})
	if err != nil {
		log.Printf("Warning: Could not fetch books from Book Service: %v", err)
		// Return author even if book service is down (graceful degradation)
		return &pb.GetAuthorBooksResponse{Author: &author}, nil
	}

	// 3. Map the books from the Book Service to the Author Service's BookSummary
	var summaries []*pb.BookSummary
	for _, b := range bookResp.Books {
		summaries = append(summaries, &pb.BookSummary{
			Id:            b.Id,
			Title:         b.Title,
			Price:         b.Price,
			PublishedYear: b.PublishedYear,
		})
	}

	return &pb.GetAuthorBooksResponse{
		Author:    &author,
		Books:     summaries,
		BookCount: int32(len(summaries)),
	}, nil
}

func (s *authorServer) CreateAuthor(ctx context.Context, req *pb.CreateAuthorRequest) (*pb.CreateAuthorResponse, error) {
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

// ... Implement GetAuthor and ListAuthors as usual ...

func main() {
	// 1. Setup Author Database
	db, err := sql.Open("sqlite3", "./authors.db")
	if err != nil {
		log.Fatal(err)
	}
	db.Exec("CREATE TABLE IF NOT EXISTS authors (id INTEGER PRIMARY KEY, name TEXT, bio TEXT, birth_year INTEGER, country TEXT)")

	// 2. Dial the Book Service (connecting to port 50051)
	conn, err := grpc.Dial("localhost:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Fail to dial Book Service: %v", err)
	}
	defer conn.Close()
	bookClient := pb.NewBookCatalogClient(conn)

	// 3. Start the Author Server on port 50052
	lis, err := net.Listen("tcp", ":50052")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterAuthorCatalogServer(grpcServer, &authorServer{db: db, bookClient: bookClient})

	log.Println("Author Service running on :50052...")
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
