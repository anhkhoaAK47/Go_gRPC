package main

import (
	"context"
	"fmt"
	"log"
	"math"
	"net"
	
	pb "book-catalog-grpc/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type calculatorServer struct {
	pb.UnimplementedCalculatorServer
	history []string  // Store calculation history
}

func (s *calculatorServer) Calculate(ctx context.Context, req *pb.CalculateRequest) (*pb.CalculateResponse, error) {
	log.Printf("Calculate: %.2f %s %.2f", req.A, req.Operation, req.B)
	
	var result float32
	
	// TODO: Implement operations
	switch req.Operation {
	case "add":
		// TODO: result = a + b
		result = req.A + req.B
	case "subtract":
		// TODO: result = a - b
		result = req.A - req.B
	case "multiply":
		// TODO: result = a * b
		result = req.A * req.B
	case "divide":
		// TODO: Check if b == 0, return error if so
		if req.B == 0 {
			return nil, status.Error(codes.InvalidArgument, "cannot divide by 0")
		}
		// TODO: result = a / b
		result = req.A / req.B
	default:
		return nil, status.Errorf(codes.InvalidArgument, 
			"unknown operation: %s", req.Operation)
	}
	
	// TODO: Add to history
	historyEntry := fmt.Sprintf("%.2f %s %.2f = %.2f", 
		req.A, req.Operation, req.B, result)
	s.history = append(s.history, historyEntry)
	
	return &pb.CalculateResponse{
		Result:    result,
		Operation: req.Operation,
	}, nil
}

func (s *calculatorServer) SquareRoot(ctx context.Context, req *pb.SquareRootRequest) (*pb.SquareRootResponse, error) {
	log.Printf("SquareRoot: %.2f", req.Number)
	
	// TODO: Check if number is negative
	if req.Number < 0 {
		return nil, status.Errorf(codes.InvalidArgument,
			"cannot calculate square root of negative number: %.2f", req.Number)
	}
	
	// TODO: Calculate square root
	result := float32(math.Sqrt(float64(req.Number)))
	
	// TODO: Add to history
	historyEntry := fmt.Sprintf("sqrt(%.2f) = %.2f", req.Number, result)
	s.history = append(s.history, historyEntry)
	
	return &pb.SquareRootResponse{Result: result}, nil
}

func (s *calculatorServer) GetHistory(ctx context.Context, req *pb.HistoryRequest) (*pb.HistoryResponse, error) {
	log.Println("GetHistory called")
	
	// TODO: Return history
	return &pb.HistoryResponse{
		Calculations: s.history,
		Count:        int32(len(s.history)),
	}, nil
}

func main() {
	// TODO: Create listener on port 50051
	listener, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatal("Failed to listen to :50051", err)
	}

	fmt.Println("🚀 Calculator gRPC server listening on :50051")
	
	// TODO: Create gRPC server
	grpcServer := grpc.NewServer()
	
	// TODO: Register Calculator service
	pb.RegisterCalculatorServer(grpcServer, &calculatorServer{})
	
	// TODO: Start serving
	if err := grpcServer.Serve(listener); err != nil {
		log.Fatal("Failed to server grpcServer", err)
	}
}