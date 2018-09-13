package main

import (
	"flag"
	pb "github.com/robaho/keydbr/internal/proto"
	"github.com/robaho/keydbr/server"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"log"
	"net"
)

func main() {
	dbpath := flag.String("path", "databases", "set top-level database directory")
	port := flag.String("port", ":8501", "set database tcp port")

	flag.Parse()

	lis, err := net.Listen("tcp", *port)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	pb.RegisterKeydbServer(s, server.NewServer(*dbpath))
	// Register reflection service on gRPC server.
	reflection.Register(s)
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
