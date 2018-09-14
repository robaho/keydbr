package server

import (
	"errors"
	"github.com/robaho/keydb"
	pb "github.com/robaho/keydbr/internal/proto"
	"log"
	"path/filepath"
	"sync"
)

type openDatabase struct {
	refcount int
	db       *keydb.Database
	fullpath string
}

type connstate struct {
	db  *openDatabase
	txs map[uint64]*keydb.Transaction
}

type Server struct {
	sync.Mutex
	path   string
	opendb map[string]*openDatabase
}

func NewServer(dbpath string) *Server {
	s := Server{path: dbpath, opendb: make(map[string]*openDatabase)}
	return &s
}

func (s *Server) Connection(conn pb.Keydb_ConnectionServer) error {

	state := connstate{txs: make(map[uint64]*keydb.Transaction)}

	defer s.closedb(&state, true)

	for {
		msg, err := conn.Recv()

		if err != nil {
			return err
		}

		switch msg.Request.(type) {
		case *pb.InMessage_Open:
			err = s.open(conn, &state, msg.GetRequest().(*pb.InMessage_Open).Open)
		case *pb.InMessage_Close:
			err = s.closedb(&state, false)
			reply := &pb.OutMessage_Close{Close: &pb.CloseReply{Error: toErrS(err)}}
			err = conn.Send(&pb.OutMessage{Reply: reply})
		case *pb.InMessage_Begin:
			err = s.begin(conn, &state, msg.GetRequest().(*pb.InMessage_Begin).Begin)
		case *pb.InMessage_Commit:
			err = s.commit(conn, &state, msg.GetRequest().(*pb.InMessage_Commit).Commit)
		case *pb.InMessage_Rollback:
			err = s.rollback(conn, &state, msg.GetRequest().(*pb.InMessage_Rollback).Rollback)
		case *pb.InMessage_Get:
			err = s.get(conn, &state, msg.GetRequest().(*pb.InMessage_Get).Get)
		case *pb.InMessage_Put:
			err = s.put(conn, &state, msg.GetRequest().(*pb.InMessage_Put).Put)
		}

		if err != nil {
			return err
		}
	}
}

func toErrS(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// clean up database references
func (s *Server) closedb(state *connstate, rollback bool) error {
	s.Lock()
	defer s.Unlock()

	if state.db == nil {
		return nil // already closed or never opened
	}

	fullpath := state.db.fullpath
	state.db = nil

	opendb, ok := s.opendb[fullpath]
	if !ok || opendb.refcount == 0 {
		return errors.New("database is ot open")
	}

	if rollback == true {
		for _, tx := range state.txs {
			tx.Rollback()
		}
	}

	log.Println("closing database", fullpath)
	opendb.refcount--
	if opendb.refcount == 0 {
		err := opendb.db.Close()
		delete(s.opendb, fullpath)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *Server) open(conn pb.Keydb_ConnectionServer, state *connstate, in *pb.OpenRequest) error {
	s.Lock()
	defer s.Unlock()

	log.Println("open database", in)

	fullpath := filepath.Join(s.path, in.GetDbname())

	opendb, ok := s.opendb[fullpath]
	if !ok {
		db, err := keydb.Open(fullpath, in.Create)

		if err != nil {
			return err
		}

		opendb = &openDatabase{refcount: 1, db: db, fullpath: fullpath}
		s.opendb[fullpath] = opendb
	} else {
		opendb.refcount++
		log.Println("database already open, returning ref", in)
	}

	state.db = opendb

	reply := &pb.OutMessage_Open{Open: &pb.OpenReply{Error: ""}}
	return conn.Send(&pb.OutMessage{Reply: reply})
}

func (s *Server) begin(conn pb.Keydb_ConnectionServer, state *connstate, in *pb.BeginRequest) error {

	var id uint64 = 0
	tx, err := state.db.db.BeginTX(in.Table)
	if err == nil {
		id = tx.GetID()
		state.txs[id] = tx
	}
	reply := &pb.OutMessage_Begin{Begin: &pb.BeginReply{Txid: id, Error: toErrS(err)}}
	return conn.Send(&pb.OutMessage{Reply: reply})
}
func (s *Server) commit(conn pb.Keydb_ConnectionServer, state *connstate, in *pb.CommitRequest) error {

	var err error
	tx, ok := state.txs[in.Txid]
	if !ok {
		err = errors.New("invalid tx id")
	} else {
		if in.Sync {
			err = tx.CommitSync()
		} else {
			err = tx.Commit()
		}
		if err != nil {
			delete(state.txs, in.Txid)
		}
	}

	reply := &pb.OutMessage_Commit{Commit: &pb.CommitReply{Error: toErrS(err)}}
	return conn.Send(&pb.OutMessage{Reply: reply})
}

func (s *Server) rollback(conn pb.Keydb_ConnectionServer, state *connstate, in *pb.RollbackRequest) error {

	var err error
	tx, ok := state.txs[in.Txid]
	if !ok {
		err = errors.New("invalid tx id")
	} else {
		err = tx.Rollback()
		if err != nil {
			delete(state.txs, in.Txid)
		}
	}

	reply := &pb.OutMessage_Rollback{Rollback: &pb.RollbackReply{Error: toErrS(err)}}
	return conn.Send(&pb.OutMessage{Reply: reply})
}

func (s *Server) get(conn pb.Keydb_ConnectionServer, state *connstate, in *pb.GetRequest) error {

	var err error
	var value []byte
	tx, ok := state.txs[in.Txid]
	if !ok {
		err = errors.New("invalid tx id")
	} else {
		value, err = tx.Get(in.Key)
	}

	reply := &pb.OutMessage_Get{Get: &pb.GetReply{Value: value, Error: toErrS(err)}}
	return conn.Send(&pb.OutMessage{Reply: reply})
}

func (s *Server) put(conn pb.Keydb_ConnectionServer, state *connstate, in *pb.PutRequest) error {

	var err error
	tx, ok := state.txs[in.Txid]
	if !ok {
		err = errors.New("invalid tx id")
	} else {
		err = tx.Put(in.Key, in.Value)
	}

	reply := &pb.OutMessage_Put{Put: &pb.PutReply{Error: toErrS(err)}}
	return conn.Send(&pb.OutMessage{Reply: reply})
}
