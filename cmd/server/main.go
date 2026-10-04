package main

import (
	"context"
	"log"
	"net/http"
	"os"

	"store/internal/api"
	"store/internal/router"
	"store/internal/store"
)

func main() {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		dsn = "postgres://postgres:devpass@127.0.0.1:5432/store?sslmode=disable"
	}
	addr := os.Getenv("ADDR")
	if addr == "" {
		addr = ":8080"
	}
	shards, err := router.ParseShards(os.Getenv("SHARDS"))
	if err != nil {
		log.Fatal(err)
	}

	db, err := store.Open(context.Background(), dsn)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	rdb, err := router.DialRedis(os.Getenv("REDIS_ADDR"))
	if err != nil {
		log.Printf("redis unavailable, serving without range counts: %v", err)
	}
	rt := router.New(db, shards, rdb)
	defer rt.Close()

	mux := http.NewServeMux()
	mux.Handle("GET /metrics", rt.Metrics())
	mux.Handle("/", api.New(rt))

	log.Printf("listening on %s", addr)
	log.Fatal(http.ListenAndServe(addr, mux))
}
