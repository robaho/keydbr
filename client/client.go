package client

import (
	"context"
	"errors"
	pb "github.com/robaho/keydbr/internal/proto"
	"google.golang.org/grpc"
	"time"
)

type RemoteDatabase struct {
	dbid    int32
	client  pb.KeydbClient
	timeout time.Duration
	stream  pb.Keydb_ConnectionClient
}

func Open(addr string, dbname string, createIfNeeded bool, timeout int) (*RemoteDatabase, error) {
	// Set up a connection to the server.
	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		return nil, err
	}

	client := pb.NewKeydbClient(conn)

	//timeoutSecs := time.Second * time.Duration(timeout)

	ctx := context.Background()
	stream, err := client.Connection(ctx)

	if err != nil {
		conn.Close()
		return nil, err
	}

	db := &RemoteDatabase{client: client, stream: stream}

	request := &pb.InMessage_Open{Open: &pb.OpenRequest{Dbname: dbname, Create: createIfNeeded}}

	err = stream.Send(&pb.InMessage{Request: request})
	if err != nil {
		return nil, err
	}

	msg, err := db.stream.Recv()
	if err != nil {
		return nil, err
	}

	response := msg.GetReply().(*pb.OutMessage_Open).Open

	if response.Error != "" {
		return nil, errors.New(response.Error)
	}

	return db, nil
}

func (db *RemoteDatabase) Close() error {

	defer db.stream.CloseSend()

	request := &pb.InMessage_Close{Close: &pb.CloseRequest{}}

	err := db.stream.Send(&pb.InMessage{Request: request})
	if err != nil {
		return err
	}

	msg, err := db.stream.Recv()
	if err != nil {
		return err
	}

	response := msg.GetReply().(*pb.OutMessage_Close).Close

	if response.Error != "" {
		return errors.New(response.Error)
	}

	return nil
}
