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

type RemoteTransaction struct {
	txid uint64
	db   *RemoteDatabase
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

	db.stream.CloseSend()

	return nil
}

func (db *RemoteDatabase) Begin(table string) (*RemoteTransaction, error) {

	request := &pb.InMessage_Begin{Begin: &pb.BeginRequest{Table: table}}

	err := db.stream.Send(&pb.InMessage{Request: request})
	if err != nil {
		return nil, err
	}

	msg, err := db.stream.Recv()
	if err != nil {
		return nil, err
	}

	response := msg.GetReply().(*pb.OutMessage_Begin).Begin

	if response.Error != "" {
		return nil, errors.New(response.Error)
	}

	rtx := new(RemoteTransaction)
	rtx.txid = response.Txid
	rtx.db = db

	return rtx, nil
}

func (tx *RemoteTransaction) Get(key []byte) ([]byte, error) {
	request := &pb.InMessage_Get{Get: &pb.GetRequest{Txid: tx.txid, Key: key}}

	err := tx.db.stream.Send(&pb.InMessage{Request: request})
	if err != nil {
		return nil, err
	}

	msg, err := tx.db.stream.Recv()
	if err != nil {
		return nil, err
	}

	response := msg.GetReply().(*pb.OutMessage_Get).Get

	if response.Error != "" {
		return nil, errors.New(response.Error)
	}

	return response.Value, nil
}

func (tx *RemoteTransaction) Put(key []byte, value []byte) error {
	request := &pb.InMessage_Put{Put: &pb.PutRequest{Txid: tx.txid, Key: key, Value: value}}

	err := tx.db.stream.Send(&pb.InMessage{Request: request})
	if err != nil {
		return err
	}

	msg, err := tx.db.stream.Recv()
	if err != nil {
		return err
	}

	response := msg.GetReply().(*pb.OutMessage_Put).Put

	if response.Error != "" {
		return errors.New(response.Error)
	}

	return nil
}

func (tx *RemoteTransaction) commitOption(sync bool) error {
	request := &pb.InMessage_Commit{Commit: &pb.CommitRequest{Txid: tx.txid, Sync: sync}}

	err := tx.db.stream.Send(&pb.InMessage{Request: request})
	if err != nil {
		return err
	}

	msg, err := tx.db.stream.Recv()
	if err != nil {
		return err
	}

	response := msg.GetReply().(*pb.OutMessage_Commit).Commit

	if response.Error != "" {
		return errors.New(response.Error)
	}

	return nil
}

func (tx *RemoteTransaction) Commit() error {
	return tx.commitOption(false)
}

func (tx *RemoteTransaction) CommitSync() error {
	return tx.commitOption(true)
}

func (tx *RemoteTransaction) Rollback() error {
	request := &pb.InMessage_Rollback{Rollback: &pb.RollbackRequest{Txid: tx.txid}}

	err := tx.db.stream.Send(&pb.InMessage{Request: request})
	if err != nil {
		return err
	}

	msg, err := tx.db.stream.Recv()
	if err != nil {
		return err
	}

	response := msg.GetReply().(*pb.OutMessage_Rollback).Rollback

	if response.Error != "" {
		return errors.New(response.Error)
	}

	return nil
}