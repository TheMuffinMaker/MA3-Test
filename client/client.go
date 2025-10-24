// client.go
package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	pb "../ma3test" // replace with actual path

	"google.golang.org/grpc"
)

func main() {
	var serverAddr string
	var clientID string

	flag.StringVar(&serverAddr, "server", "localhost:50051", "gRPC server address")
	flag.StringVar(&clientID, "id", "", "Client ID (required)")
	flag.Parse()

	if clientID == "" {
		fmt.Fprintln(os.Stderr, "client id is required: -id <name>")
		os.Exit(2)
	}

	conn, err := grpc.Dial(serverAddr, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Client CONNECT_ERROR: %v", err)
	}
	defer conn.Close()

	client := pb.NewChitChatServiceClient(conn)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start subscribe stream to receive broadcasts
	subReq := &pb.SubscribeRequest{ClientId: clientID}
	stream, err := client.Subscribe(ctx, subReq)
	if err != nil {
		log.Fatalf("Client SUBSCRIBE_ERROR: %v", err)
	}
	log.Printf("Client STARTUP: id=%s connected to %s", clientID, serverAddr)

	// Goroutine to receive server broadcasts
	go func() {
		for {
			b, err := stream.Recv()
			if err != nil {
				log.Printf("Client STREAM_RECV_ERROR: id=%s err=%v", clientID, err)
				return
			}
			// Display + log message content and logical timestamp
			display := fmt.Sprintf(">> [%d] %s", b.GetLogicalTime(), b.GetContent())
			fmt.Println(display)
			log.Printf("Client MESSAGE_RECEIVED: id=%s logical_time=%d content=%q", clientID, b.GetLogicalTime(), b.GetContent())
		}
	}()

	// Read lines from stdin and publish
	stdin := bufio.NewScanner(os.Stdin)
	fmt.Println("Type messages and press Enter to publish. Type '/leave' to exit.")
	for stdin.Scan() {
		line := stdin.Text()
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		if line == "/leave" {
			// call Leave RPC then exit
			_, err := client.Leave(context.Background(), &pb.LeaveRequest{ClientId: clientID})
			if err != nil {
				log.Printf("Client LEAVE_RPC_ERROR: %v", err)
			}
			log.Printf("Client SHUTDOWN: id=%s initiated leave", clientID)
			// Sleep briefly to allow leave broadcast to flow and then exit
			time.Sleep(200 * time.Millisecond)
			return
		}

		// Publish
		resp, err := client.Publish(context.Background(), &pb.PublishRequest{ClientId: clientID, Content: line})
		if err != nil {
			log.Printf("Client PUBLISH_ERROR: %v", err)
			continue
		}
		if !resp.Ok {
			log.Printf("Client PUBLISH_REJECTED: reason=%s", resp.Error)
			continue
		}
		log.Printf("Client PUBLISH_SENT: id=%s content=%q", clientID, line)
	}

	if stdin.Err() != nil {
		log.Printf("Client STDIN_ERROR: %v", stdin.Err())
	}
}
