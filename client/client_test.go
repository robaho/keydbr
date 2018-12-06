package client_test

import (
	"github.com/robaho/keydbr/client"
	"log"
	"testing"
)

var addr = "localhost:8501"
var dbname = "main"

func TestBasic(t *testing.T) {

	db, err := client.Open(addr, dbname, true, 10)
	if err != nil {
		log.Fatal(err)
	}

	tx, err := db.BeginTX("test")
	if err != nil {
		t.Fatal(err)
	}

	err = tx.Put([]byte("mykey"), []byte("myvalue"))
	if err != nil {
		t.Fatal(err)
	}

	val, err := tx.Get([]byte("mykey"))
	if err != nil {
		t.Fatal(err)
	}

	if string(val) != "myvalue" {
		t.Fatal("wrong value returned", string(val))
	}

	err = tx.Commit()
	if err != nil {
		t.Fatal(err)
	}

	err = db.Close()
	if err != nil {
		log.Fatal(err)
	}
}

func TestRollback(t *testing.T) {

	db, err := client.Open(addr, dbname, true, 10)
	if err != nil {
		log.Fatal(err)
	}

	tx, err := db.BeginTX("test")
	if err != nil {
		t.Fatal(err)
	}

	err = tx.Put([]byte("mykey"), []byte("myvalue"))
	if err != nil {
		t.Fatal(err)
	}

	val, err := tx.Get([]byte("mykey"))
	if err != nil {
		t.Fatal(err)
	}

	if string(val) != "myvalue" {
		t.Fatal("wrong value returned", string(val))
	}

	err = db.Close()
	if err == nil {
		log.Fatal("commit should fail with open tx")
	}

	err = tx.Rollback()
	if err != nil {
		t.Fatal(err)
	}
	err = db.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestLookup(t *testing.T) {

	db, err := client.Open(addr, dbname, true, 10)
	if err != nil {
		log.Fatal(err)
	}

	tx, err := db.BeginTX("test")
	if err != nil {
		t.Fatal(err)
	}

	err = tx.Put([]byte("mykey"), []byte("myvalue"))
	if err != nil {
		t.Fatal(err)
	}

	val, err := tx.Get([]byte("mykey"))
	if err != nil {
		t.Fatal(err)
	}

	err = tx.Commit()
	if err != nil {
		t.Fatal(err)
	}

	tx, err = db.BeginTX("test")
	if err != nil {
		t.Fatal(err)
	}

	itr, err := tx.Lookup(nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	key, val, err := itr.Next()
	if err != nil {
		t.Fatal(err)
	}

	if string(key) != "mykey" {
		t.Fatal("wrong key returned", string(val))
	}
	if string(val) != "myvalue" {
		t.Fatal("wrong value returned", string(val))
	}

	_, _, err = itr.Next()
	if err == nil {
		t.Fatal("should of been end of iterator")
	}

	err = tx.Commit()
	if err != nil {
		t.Fatal(err)
	}

	err = db.Close()
	if err != nil {
		log.Fatal(err)
	}
}
