package client_test

import (
	"github.com/robaho/keydbr/client"
	"log"
	"testing"
)

func TestBasic(t *testing.T) {

	addr := "localhost:8501"
	dbname := "main"

	db, err := client.Open(addr, dbname, true, 10)
	if err != nil {
		log.Fatal(err)
	}

	tx, err := db.Begin("test")
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
