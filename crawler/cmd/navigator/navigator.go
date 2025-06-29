package main

import (
	"bufio"
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
	MAX_NAVIGATORS = 30
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

	client := &http.Client{
		Timeout: 15 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        1,
			MaxIdleConnsPerHost: 1,
		},
	}

	rateTicker := time.NewTicker(INTERVAL)
	defer rateTicker.Stop()

	var wg sync.WaitGroup
	urlChan := make(chan string, MAX_NAVIGATORS*2)
	results := make(chan *Post, MAX_NAVIGATORS*2)

	for i := range MAX_NAVIGATORS {
		wg.Add(1)
		go navigate(i, &wg, client, urlChan, results, rateTicker)
	}

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

		writer.Flush()
	}()

	for _, u := range urls {
		urlChan <- u
	}

	close(urlChan)
	wg.Wait()
	close(results)
}

func navigate(workerID int, wg *sync.WaitGroup, client *http.Client, urls <-chan string, results chan<- *Post, ticker *time.Ticker) {
	defer wg.Done()

	log.Printf("worker %d starting\n", workerID)

	for u := range urls {
		var post *Post
		var content string
		var err error

		for attempt := 1; attempt <= RETRIES; attempt++ {
			<-ticker.C

			post, content, err = readPost(client, u)
			if err == nil {
				break
			}

			if attempt < RETRIES {
				log.Printf("worker=%d url=%s attempt=%d error=%v, retrying", workerID, u, attempt, err)
				time.Sleep(time.Duration(attempt+1) * time.Second)
				continue
			}
			break
		}

		if err != nil || post == nil {
			fmt.Printf("Failed on url navigation %s : %v\n", u, err)
			continue
		}

		contentPath, err := writeContentToFile(u, content)
		if err != nil {
			log.Printf("worker=%d failed to save content %s: %v\n", workerID, u, err)
		} else {
			post.ContentPath = contentPath
		}

		log.Printf("worker %d extracted successfully %s", workerID, contentPath)
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
	infoWrapper.Find("a.crayons-tag").Each(func(_ int, s *goquery.Selection) {
		tags = append(tags, strings.TrimSpace(s.Text()))
	})
	p.Tags = strings.Join(tags, "/")

	content := strings.TrimSpace(doc.Find(".crayons-article__main").Text())

	return p, content, nil
}

func writeContentToFile(postUrl string, content string) (string, error) {
	u, err := url.Parse(postUrl)
	if err != nil {
		return "", err
	}

	segments := strings.Split(strings.Trim(u.Path, "/"), "/")
	slug := "post"
	if len(segments) > 0 {
		slug = segments[len(segments)-1]
	}

	slug = strings.ReplaceAll(slug, " ", "_")
	filename := slug + ".txt"
	path := filepath.Join(BLOG_DIR, filename)

	i := 1
	for {
		if _, err := os.Stat(path); os.IsNotExist(err) {
			break
		}
		path = filepath.Join(BLOG_DIR, fmt.Sprintf("%s_%d.txt", strings.TrimSuffix(slug, ".txt"), i))
		i++
	}

	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		return "", err
	}

	return path, nil
}

func readURLs(filename string) ([]string, error) {
	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var urls []string
	r := bufio.NewReader(f)
	for {
		line, err := r.ReadString('\n')
		if err != nil && err != io.EOF {
			return nil, err
		}
		line = strings.TrimSpace(line)
		if line != "" {
			urls = append(urls, line)
		}
		if err == io.EOF {
			break
		}
	}

	return urls, nil
}
