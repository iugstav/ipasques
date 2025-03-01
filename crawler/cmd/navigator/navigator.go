package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/PuerkitoBio/goquery"
	"iugstav.ipasques/robot"
)

const (
	MAX_NAVIGATORS = 10
	TIMEOUT        = 10 * time.Second
	INTERVAL       = 900 * time.Millisecond
	RETRIES        = 3
	BLOG_DIR       = "posts"
)

type Post struct {
	URL         string `json:"url"`
	Title       string `json:"title"`
	Author      string `json:"author"`
	PublishedAt string `json:"published_at"`
	ContentPath string `json:"content"`
	Tags        string `json:"tags"`
}

type RateLimiter struct {
	lastReq  time.Time
	mu       sync.Mutex
	interval time.Duration
}

func NewRateLimiter() *RateLimiter {
	return &RateLimiter{interval: INTERVAL}
}

func (r *RateLimiter) Wait() {
	r.mu.Lock()
	defer r.mu.Unlock()

	elapsedTime := time.Since(r.lastReq)
	if elapsedTime < r.interval {
		time.Sleep(r.interval - elapsedTime)
	}
	r.lastReq = time.Now()
}

func main() {
	if len(os.Args) != 3 {
		log.Fatalln("Usage: ./scraper <input-file> <output-file>")
	}

	urls, err := readURLs(os.Args[1])
	if err != nil {
		log.Fatal(err)
	}

	output, err := os.Create(os.Args[2])
	if err != nil {
		log.Fatal(err)
	}
	defer output.Close()

	if err := os.MkdirAll(BLOG_DIR, 0755); err != nil {
		log.Fatalln(err)
	}

	writer := csv.NewWriter(output)
	defer writer.Flush()
	if err := writer.Write([]string{
		"url", "title", "author", "published_at", "content_path", "tags",
	}); err != nil {
		log.Fatal(err)
	}

	rl := &RateLimiter{interval: INTERVAL}
	client := &http.Client{
		Timeout: 15 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        1,
			MaxIdleConnsPerHost: 1,
		},
	}

	var wg sync.WaitGroup
	mu := &sync.Mutex{}
	urlChan := make(chan string, MAX_NAVIGATORS*2)
	results := make(chan *Post, MAX_NAVIGATORS*2)

	// go writeResults(output, results)

	go func() {
		for post := range results {
			record := []string{
				post.URL,
				post.Title,
				post.Author,
				post.PublishedAt,
				post.ContentPath,
				post.Tags,
			}
			if err := writer.Write(record); err != nil {
				log.Printf("CSV write error: %v\n", err)
			}
		}
	}()

	for range MAX_NAVIGATORS {
		wg.Add(1)
		go navigate(&wg, mu, client, urlChan, results, rl)
	}

	for _, u := range urls {
		urlChan <- u
	}

	close(urlChan)
	wg.Wait()
	close(results)
}

func navigate(wg *sync.WaitGroup, mu *sync.Mutex, client *http.Client, urls <-chan string, results chan<- *Post, rlim *RateLimiter) {
	defer wg.Done()

	for u := range urls {
		var post *Post
		var content string
		var err error

		for retry := range RETRIES {
			rlim.Wait()
			post, content, err = readPost(client, u)
			if err == nil {
				break
			}

			if shouldRetry(err) {
				time.Sleep(time.Duration(retry+1) * time.Second)
				continue
			}
			break
		}

		if err != nil || post == nil {
			fmt.Printf("Failed on url navigation %s : %v\n", u, err)
			continue
		}

		contentPath, err := writeContentToFile(u, content, mu)
		if err != nil {
			log.Printf("failed to save content for post %s: %v\n", u, err)
		}
		post.ContentPath = contentPath

		results <- post
	}

}

func readPost(c *http.Client, url string) (*Post, string, error) {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, "", err
	}
	req.Header.Set("User-Agent", robot.PickUA())

	res, err := c.Do(req)
	if err != nil {
		return nil, "", err
	}
	defer res.Body.Close()

	if res.StatusCode != 200 {
		return nil, "", fmt.Errorf("invalid status code %d\n", res.StatusCode)
	}

	return parse(res.Body, url)
}

func parse(body io.Reader, url string) (*Post, string, error) {
	doc, err := goquery.NewDocumentFromReader(body)
	if err != nil {
		return nil, "", err
	}

	p := &Post{URL: url}

	infoWrapper := doc.Find("div.crayons-article__header__meta")
	p.Title = strings.TrimSpace(infoWrapper.Find("h1").Text())
	p.Author = strings.TrimSpace(infoWrapper.Find("a.crayons-link").Text())
	if timeEl := infoWrapper.Find("time"); timeEl.Length() > 0 {
		if pubDate, exists := timeEl.Attr("datetime"); exists {
			p.PublishedAt = pubDate
		}
	}

	var tags []string
	infoWrapper.Find("a.crayons-tag").Each(func(i int, s *goquery.Selection) {
		tags = append(tags, strings.TrimSpace(s.Text()))
	})
	p.Tags = strings.Join(tags, "/")

	content := strings.TrimSpace(doc.Find(".crayons-article__main").Text())

	return p, content, nil
}

func writeContentToFile(postUrl string, content string, mu *sync.Mutex) (string, error) {
	u, err := url.Parse(postUrl)
	if err != nil {
		return "", err
	}

	segments := strings.Split(u.Path, "/")
	filename := "post"
	if len(segments) > 0 {
		filename = segments[len(segments)-1]
	}

	filename = strings.ReplaceAll(filename, " ", "_")
	filename = strings.ReplaceAll(filename, "/", "-")
	filename = strings.TrimSuffix(filename, ".html") + ".txt"
	path := filepath.Join(BLOG_DIR, filename)

	mu.Lock()
	defer mu.Unlock()

	if _, err := os.Stat(path); !os.IsNotExist(err) {
		for i := 1; ; i++ {
			newPath := fmt.Sprintf("%s_%d%s",
				strings.TrimSuffix(path, ".txt"),
				i,
				".txt")
			if _, err := os.Stat(newPath); os.IsNotExist(err) {
				path = newPath
				break
			}
		}
	}

	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		return "", err
	}

	return path, nil
}

func readURLs(filename string) ([]string, error) {
	content, err := os.ReadFile(filename)
	if err != nil {
		return nil, err
	}
	return strings.Fields(string(content)), nil
}

func shouldRetry(err error) bool {
	if e, ok := err.(interface{ Timeout() bool }); ok && e.Timeout() {
		return true
	}
	return false
}
