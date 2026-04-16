package main

import (
	"context"
	"fmt"
	"log"
	"time"

	pb "book-catalog-grpc/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

func main() {
	// TODO: Connect to server
	conn, err := grpc.NewClient("localhost:50051",
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	client := pb.NewCalculatorClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Test 1: Addition
	fmt.Println("=== Test 1: Addition ===")
	resp, err := client.Calculate(ctx, &pb.CalculateRequest{
		A:         10,
		B:         5,
		Operation: "add",
	})
	// TODO: Handle error and print result
	if err != nil {
		log.Fatal("Calculation add failed: ", err)
	}

	fmt.Printf("Result: %.2f + %.2f = %.2f", 10.0, 5.0, resp.Result)

	// Test 2: Division
	fmt.Println("\n=== Test 2: Division ===")
	// TODO: Test 20 / 4
	resp, err = client.Calculate(ctx, &pb.CalculateRequest{
		A:         20,
		B:         4,
		Operation: "divide",
	})
	if err != nil {
		log.Fatal("Calculation divide failed: ", err)
	}

	fmt.Printf("Result: %.2f / %.2f = %.2f", 20.0, 4.0, resp.Result)

	// Test 3: Division by zero (should error)
	fmt.Println("\n=== Test 3: Division by Zero ===")
	_, err = client.Calculate(ctx, &pb.CalculateRequest{
		A:         10,
		B:         0,
		Operation: "divide",
	})
	if err != nil {
		st, _ := status.FromError(err)
		fmt.Printf("Expected error: %s\n", st.Message())
	}

	// Test 4: Square root
	fmt.Println("\n=== Test 4: Square Root ===")
	// TODO: Test sqrt(16)
	sqrtResp, err := client.SquareRoot(ctx, &pb.SquareRootRequest{
		Number: 16,
	})
	if err != nil {
		log.Fatal("Square root calculation failed: ", err)
	}

	fmt.Printf("Result: sqrt(%.2f) = %.2f", 16.0, sqrtResp.Result)

	// Test 5: Negative square root (should error)
	fmt.Println("\n=== Test 5: Negative Square Root ===")
	// TODO: Test sqrt(-4), should error
	_, err = client.SquareRoot(ctx, &pb.SquareRootRequest{
		Number: -4,
	})
	if err != nil {
		st, _ := status.FromError(err)
		fmt.Printf("Expected error: %s\n", st.Message())
	}

	// Test 6: Get history
	fmt.Println("\n=== Test 6: History ===")
	// TODO: Get and print all calculations
	historyResp, err := client.GetHistory(ctx, &pb.HistoryRequest{})

	if err != nil {
		log.Fatal("Failed to get history: ", err)
	}

	for i, h := range historyResp.GetCalculations() {
		fmt.Printf("%d. %s\n", i+1, h)
	}
}
