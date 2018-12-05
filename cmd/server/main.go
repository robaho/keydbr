package main

import (
	"flag"
	"fmt"
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

	s := grpc.NewServer(grpc.StreamInterceptor(streamInterceptor))
	pb.RegisterKeydbServer(s, server.NewServer(*dbpath))
	// Register reflection service on gRPC server.
	reflection.Register(s)
	fmt.Println("listening on ", lis.Addr())
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}

func streamInterceptor(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	log.Printf("stream opened - Context:%s\n", ss.Context())

	err := handler(srv, ss)

	log.Printf("stream closed - Context:%s\n", ss.Context())

	return err
}
