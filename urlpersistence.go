package main

import (
	"fmt"
	"os"
	"sync"
	"time"
)

type URLWriter struct {
	file      *os.File
	writeChan chan string
	done      chan struct{}
	wg        sync.WaitGroup
}

func InitURLWriter(filename string) (*URLWriter, error) {
	f, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}

	w := &URLWriter{
		file:      f,
		writeChan: make(chan string, 1000),
		done:      make(chan struct{}),
	}

	w.wg.Add(1)
	go w.run()
	return w, nil
}

func (w *URLWriter) Write(url string) {
	select {
	case w.writeChan <- url:
	case <-w.done:
	}
}

func (w *URLWriter) run() {
	defer w.wg.Done()

	batchWrite := make([]string, 0, 100)
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case url := <-w.writeChan:
			batchWrite = append(batchWrite, url)
			if len(batchWrite) >= 100 {
				w.flush(batchWrite)
				batchWrite = batchWrite[:0]
			}

		case <-ticker.C:
			if len(batchWrite) > 0 {
				w.flush(batchWrite)
				batchWrite = batchWrite[:0]
			}

		case <-w.done:
			if len(batchWrite) > 0 {
				w.flush(batchWrite)
			}
			return
		}
	}
}

func (w *URLWriter) flush(urls []string) {
	for _, u := range urls {
		if _, err := w.file.WriteString(u + "\n"); err != nil {
			fmt.Println(fmt.Errorf("error writing urls to file : %v", err))
		}
	}
}

func (w *URLWriter) Close() error {
	close(w.done)
	w.wg.Wait()
	return w.file.Close()
}
