package main

import (
	"fmt"
	"math/rand"
	"os"
	"sync"
	"time"

	"iugstav.ipasques/robot"
)

func main() {
	rand.New(rand.NewSource(time.Now().UnixNano()))

	w, err := robot.InitURLWriter("devto_urls.txt")
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	defer w.File.Close()

	crawler := robot.InitCrawler()
	defer crawler.Cleanup()

	frontier := robot.NewCrawlerFrontier(1 * time.Second)
	defer frontier.Close()

	crawler.GetTags(frontier)

	var wg sync.WaitGroup
	for i := range robot.WORKER_COUNT {
		wg.Add(1)
		go func(id int, wg *sync.WaitGroup) {
			defer wg.Done()

			browser := crawler.GetBrowser()
			defer crawler.Pool.Put(browser)
			for {
				item := frontier.Next()
				if item == nil {
					fmt.Printf("Worker %d | Shutting down...\n", id)
					return
				}

				fmt.Printf("Worker %d at tag %s\n", id, item.URL)
				robot.ProcessTag(id, item, frontier, browser, w)
			}
		}(i, &wg)
	}

	frontier.Close()
	wg.Wait()
}
