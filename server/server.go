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

type state struct {
	dbname string
}

type Server struct {
	sync.Mutex
	path   string
	opendb map[string]*openDatabase
	nextid int
	states map[int]*state // map of connection id to connection state
}

func NewServer(dbpath string) *Server {
	s := Server{path: dbpath, opendb: make(map[string]*openDatabase), states: make(map[int]*state)}
	return &s
}

func (s *Server) Connection(conn pb.Keydb_ConnectionServer) error {

	s.Lock()
	s.nextid++
	id := s.nextid
	s.Unlock()

	state := state{}

	s.states[id] = &state

	defer s.closedb(id)

	for {
		msg, err := conn.Recv()

		if err != nil {
			return err
		}

		switch msg.Request.(type) {
		case *pb.InMessage_Open:
			err = s.open(conn, id, msg.GetRequest().(*pb.InMessage_Open).Open)
		case *pb.InMessage_Close:
			err = s.closedb(id)
			reply := &pb.OutMessage_Close{Close: &pb.CloseReply{Error: toErrS(err)}}
			err = conn.Send(&pb.OutMessage{Reply: reply})
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
func (s *Server) closedb(id int) error {
	s.Lock()
	defer s.Unlock()

	state, ok := s.states[id]
	if !ok {
		return nil
	}

	fullpath := state.dbname

	defer delete(s.states, id)

	opendb, ok := s.opendb[fullpath]
	if !ok || opendb.refcount == 0 {
		return errors.New("database is ot open")
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

func (s *Server) open(conn pb.Keydb_ConnectionServer, id int, in *pb.OpenRequest) error {
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

	s.states[id].dbname = fullpath

	reply := &pb.OutMessage_Open{Open: &pb.OpenReply{Error: ""}}

	err := conn.Send(&pb.OutMessage{Reply: reply})

	return err
}
