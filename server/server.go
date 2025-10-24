// server.go
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"sync"
	"unicode/utf8"

	pb "../ma3test" // replace with actual module/path after generation

	"google.golang.org/grpc"
)

const (
	maxMsgChars     = 128
	subscriberBuf   = 64
	defaultBindAddr = ":50051"
)

type server struct {
	pb.UnimplementedChitChatServiceServer

	mu          sync.Mutex
	subscribers map[string]*subscriber // clientID -> subscriber
	clock       int64                  // Lamport clock
}

type subscriber struct {
	id     string
	ch     chan *pb.Broadcast
	done   chan struct{}
	stream pb.ChitChatService_SubscribeServer
}

func newServer() *server {
	return &server{
		subscribers: make(map[string]*subscriber),
		clock:       0,
	}
}

func (s *server) incrementClock() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.clock++
	return s.clock
}

func (s *server) Subscribe(req *pb.SubscribeRequest, stream pb.ChitChatService_SubscribeServer) error {
	clientID := req.GetClientId()
	if clientID == "" {
		return errors.New("client_id required")
	}

	// create subscriber
	sub := &subscriber{
		id:     clientID,
		ch:     make(chan *pb.Broadcast, subscriberBuf),
		done:   make(chan struct{}),
		stream: stream,
	}

	// register subscriber
	s.mu.Lock()
	if _, exists := s.subscribers[clientID]; exists {
		s.mu.Unlock()
		// Already connected? reject
		return fmt.Errorf("client %s already subscribed", clientID)
	}
	s.subscribers[clientID] = sub
	s.mu.Unlock()

	log.Printf("Server CLIENT_CONNECTED: id=%s", clientID)

	// Start goroutine to write from sub.ch to the gRPC stream
	var writeWg sync.WaitGroup
	writeWg.Add(1)
	go func() {
		defer writeWg.Done()
		for {
			select {
			case b, ok := <-sub.ch:
				if !ok {
					return
				}
				// send; if send fails, we treat as client disconnect
				if err := stream.Send(b); err != nil {
					log.Printf("Server SEND_ERROR: id=%s err=%v", clientID, err)
					return
				}
				// Log delivery
				log.Printf("Server MESSAGE_DELIVERED: to=%s type=%v logical_time=%d content=%q",
					clientID, b.GetType(), b.GetLogicalTime(), b.GetContent())
			case <-sub.done:
				return
			}
		}
	}()

	// Broadcast join message (must be delivered to all including this new one)
	lt := s.incrementClock()
	joinContent := fmt.Sprintf("Participant %s joined Chit Chat at logical time %d", clientID, lt)
	b := &pb.Broadcast{
		Type:        pb.Broadcast_JOIN,
		ClientId:    clientID,
		Content:     joinContent,
		LogicalTime: lt,
	}
	s.broadcast(b)

	// Wait until the stream context is done (client closes) or server side finishes
	<-stream.Context().Done()

	// Clean up: remove subscriber, stop write goroutine
	close(sub.done)
	// ensure channel is closed so writer goroutine can return
	s.mu.Lock()
	delete(s.subscribers, clientID)
	close(sub.ch)
	s.mu.Unlock()

	// Broadcast leave message
	lt2 := s.incrementClock()
	leaveContent := fmt.Sprintf("Participant %s left Chit Chat at logical time %d", clientID, lt2)
	b2 := &pb.Broadcast{
		Type:        pb.Broadcast_LEAVE,
		ClientId:    clientID,
		Content:     leaveContent,
		LogicalTime: lt2,
	}
	s.broadcast(b2)
	log.Printf("Server CLIENT_DISCONNECTED: id=%s logical_time=%d", clientID, lt2)

	// wait for writer to exit
	writeWg.Wait()
	return nil
}

func (s *server) Publish(ctx context.Context, req *pb.PublishRequest) (*pb.PublishResponse, error) {
	clientID := req.GetClientId()
	content := req.GetContent()

	// validate client id
	if clientID == "" {
		return &pb.PublishResponse{Ok: false, Error: "client_id required"}, nil
	}
	// validate UTF-8
	if !utf8.ValidString(content) {
		return &pb.PublishResponse{Ok: false, Error: "content is not valid UTF-8"}, nil
	}
	// validate length (in runes/characters)
	if utf8.RuneCountInString(content) > maxMsgChars {
		return &pb.PublishResponse{Ok: false, Error: fmt.Sprintf("content exceeds %d characters", maxMsgChars)}, nil
	}

	lt := s.incrementClock()
	b := &pb.Broadcast{
		Type:        pb.Broadcast_CHAT,
		ClientId:    clientID,
		Content:     fmt.Sprintf("%s", content),
		LogicalTime: lt,
	}

	// Log server-side that it received a publish
	log.Printf("Server PUBLISH_RECEIVED: from=%s logical_time=%d content=%q", clientID, lt, content)

	s.broadcast(b)
	return &pb.PublishResponse{Ok: true}, nil
}

func (s *server) Leave(ctx context.Context, req *pb.LeaveRequest) (*pb.LeaveResponse, error) {
	clientID := req.GetClientId()
	if clientID == "" {
		return &pb.LeaveResponse{Ok: false, Error: "client_id required"}, nil
	}

	// If subscriber exists, close its stream by canceling its done channel and removing it.
	s.mu.Lock()
	sub, ok := s.subscribers[clientID]
	if ok {
		delete(s.subscribers, clientID)
		close(sub.done) // writer goroutine will exit when channel closed
		close(sub.ch)
	}
	s.mu.Unlock()

	if ok {
		lt := s.incrementClock()
		leaveContent := fmt.Sprintf("Participant %s left Chit Chat at logical time %d", clientID, lt)
		b := &pb.Broadcast{
			Type:        pb.Broadcast_LEAVE,
			ClientId:    clientID,
			Content:     leaveContent,
			LogicalTime: lt,
		}
		s.broadcast(b)
		log.Printf("Server CLIENT_LEFT (via Leave RPC): id=%s logical_time=%d", clientID, lt)
		return &pb.LeaveResponse{Ok: true}, nil
	}
	return &pb.LeaveResponse{Ok: false, Error: "client not connected"}, nil
}

// broadcast sends the broadcast to every subscriber's channel (non-blocking).
func (s *server) broadcast(b *pb.Broadcast) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for id, sub := range s.subscribers {
		select {
		case sub.ch <- b:
			// queued successfully
		default:
			// channel full — log and drop to avoid blocking
			log.Printf("Server BROADCAST_DROP: to=%s logical_time=%d reason=chan_full", id, b.GetLogicalTime())
		}
	}
	// Also log broadcast event
	log.Printf("Server BROADCAST: type=%v logical_time=%d content=%q", b.GetType(), b.GetLogicalTime(), b.GetContent())
}

func main() {
	log.SetFlags(log.LstdFlags)
	addr := defaultBindAddr

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("Server STARTUP_ERROR: failed to listen on %s: %v", addr, err)
	}
	grpcServer := grpc.NewServer()
	s := newServer()
	pb.RegisterChitChatServiceServer(grpcServer, s)

	log.Printf("Server STARTUP: listening on %s", addr)

	// run server
	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("Server ERROR: %v", err)
		}
	}()

	// Wait for interrupt (simple, demo uses sleep to keep running).
	// In a real app you would trap signals. For the assignment we'll log and exit on CTRL+C
	// but here we will just block indefinitely.
	select {}
}
