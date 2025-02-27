package main

import (
	"fmt"
	"math/rand"
	"os"
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

	w, err := InitURLWriter("devto_urls.txt")
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

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

				fmt.Printf("Worker %d at tag %s\n", id, item.URL)
				ProcessTag(id, item, frontier, browser, w)
			}
		}(i, &wg)
	}

	frontier.Close()
	wg.Wait()
	fmt.Println("fechou paizão")
}
