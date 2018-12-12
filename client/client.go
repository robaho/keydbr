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
type RemoteIterator struct {
	id uint64
	db *RemoteDatabase
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

	response := msg.GetOpen()

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

	response := msg.GetClose()

	if response.Error != "" {
		return errors.New(response.Error)
	}

	db.stream.CloseSend()

	return nil
}

func Remove(addr string, dbname string, timeout int) error {
	// Set up a connection to the server.
	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		return err
	}

	defer conn.Close()

	client := pb.NewKeydbClient(conn)

	//timeoutSecs := time.Second * time.Duration(timeout)

	ctx := context.Background()

	request := &pb.RemoveRequest{}
	request.Dbname = dbname

	response, err := client.Remove(ctx, request)

	if err != nil {
		return err
	}

	if response.Error != "" {
		return errors.New(response.Error)
	}
	return nil
}

func (db *RemoteDatabase) BeginTX(table string) (*RemoteTransaction, error) {

	request := &pb.InMessage_Begin{Begin: &pb.BeginRequest{Table: table}}

	err := db.stream.Send(&pb.InMessage{Request: request})
	if err != nil {
		return nil, err
	}

	msg, err := db.stream.Recv()
	if err != nil {
		return nil, err
	}

	response := msg.GetBegin()

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

	response := msg.GetGet()

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

	response := msg.GetPut()

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

	response := msg.GetCommit()

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

	response := msg.GetRollback()

	if response.Error != "" {
		return errors.New(response.Error)
	}

	return nil
}

func (tx *RemoteTransaction) Lookup(lower []byte, upper []byte) (*RemoteIterator, error) {
	request := &pb.InMessage_Lookup{Lookup: &pb.LookupRequest{Txid: tx.txid, Lower: lower, Upper: upper}}

	err := tx.db.stream.Send(&pb.InMessage{Request: request})
	if err != nil {
		return nil, err
	}

	msg, err := tx.db.stream.Recv()
	if err != nil {
		return nil, err
	}

	response := msg.GetLookup()

	if response.Error != "" {
		return nil, errors.New(response.Error)
	}

	ri := RemoteIterator{id: response.Id, db: tx.db}

	return &ri, nil
}

func (itr *RemoteIterator) Next() (key []byte, value []byte, err error) {
	request := &pb.InMessage_Next{Next: &pb.LookupNextRequest{Id: itr.id}}

	err = itr.db.stream.Send(&pb.InMessage{Request: request})
	if err != nil {
		return nil, nil, err
	}

	msg, err := itr.db.stream.Recv()
	if err != nil {
		return nil, nil, err
	}

	response := msg.GetNext()

	if response.Error != "" {
		return nil, nil, errors.New(response.Error)
	}

	return response.Entries[0].Key, response.Entries[0].Value, nil
}
