package main

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

const (
	WORKER_COUNT = 100 // one for each tag
	MAX_DEPTH    = 3
	TIMEOUT      = 10 * time.Second
)

func main() {
	rand.New(rand.NewSource(time.Now().UnixNano()))

	crawler := InitCrawler()
	defer crawler.Cleanup()

	frontier := NewCrawlerFrontier(1 * time.Second)
	defer frontier.Close()

	crawler.GetTags(frontier)

	var wg sync.WaitGroup
	for i := range WORKER_COUNT {
		wg.Add(1)
		go func(id int, wg *sync.WaitGroup) {
			defer wg.Done()

			browser := crawler.GetBrowser()
			defer crawler.pool.Put(browser)
			for {
				item := frontier.Next()
				if item == nil {
					fmt.Printf("Worker %d | Shutting down...\n", id)
					return
				}
				ProcessTag(id, item, frontier, browser)
			}
		}(i, &wg)
	}

	wg.Wait()
	fmt.Println("fechou paizão")
}
