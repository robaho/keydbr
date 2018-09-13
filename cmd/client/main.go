package main

import (
	"flag"
	"github.com/robaho/keydbr/client"
	"log"
	"time"
)

func main() {
	addr := flag.String("addr", "localhost:8501", "set the remote database address")
	dbname := flag.String("db", "main", "set the remote database name")
	create := flag.Bool("c", true, "create if needed")
	timeout := flag.Int("t", 5, "number of seconds before timeout")

	flag.Parse()

	db, err := client.Open(*addr, *dbname, *create, *timeout)
	if err != nil {
		log.Fatal(err)
	}

	time.Sleep(time.Second * 2)

	err = db.Close()
	if err != nil {
		log.Fatal(err)
	}
}
