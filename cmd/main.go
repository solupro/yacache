package main

import (
	"fmt"
	"log"
	"net/http"
	"yacache"
)

var db = map[string]string{
	"Tom":  "630",
	"Jack": "589",
	"solu": "567",
}

func main() {
	yacache.NewGroup("scores", 2<<10, yacache.GetterFunc(
		func(key string) ([]byte, error) {
			log.Printf("[SlowDB] search key", key)
			if v, ok := db[key]; ok {
				return []byte(v), nil
			}
			return nil, fmt.Errorf("%s not exist", key)
		}))

	addr := "localhost:9999"
	peers := yacache.NewHTTPPool(addr)
	log.Println("yacache is running at", addr)
	log.Fatal(http.ListenAndServe(addr, peers))
}