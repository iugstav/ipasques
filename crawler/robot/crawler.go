package robot

import (
	"container/heap"
	"fmt"
	"log"
	"math/rand"
	"net/url"
	"sync"
	"time"

	"github.com/go-rod/rod"
	"github.com/go-rod/rod/lib/launcher"
	"github.com/go-rod/rod/lib/proto"
)

const (
	WORKER_COUNT = 100 // one for each tag
	MAX_DEPTH    = 3
	TIMEOUT      = 10 * time.Second
)

type Item struct {
	URL       string
	Priority  int
	Depth     int
	Domain    string
	Timestamp int64
	index     int
}

func NewItem(rawURL string, depth int) (*Item, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, err
	}

	return &Item{
		URL:       rawURL,
		Priority:  depth,
		Depth:     depth,
		Domain:    u.Hostname(),
		Timestamp: time.Now().Unix(),
	}, nil
}

// Priority Queue using heap
type PQueue []*Item

func (q PQueue) Len() int {
	return len(q)
}

func (pq PQueue) Less(i, j int) bool {
	// First priority, then depth, then timestamp
	if pq[i].Priority == pq[j].Priority {
		if pq[i].Depth == pq[j].Depth {
			return pq[i].Timestamp < pq[j].Timestamp
		}
		return pq[i].Depth < pq[j].Depth
	}
	return pq[i].Priority < pq[j].Priority
}

func (pq PQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

func (pq *PQueue) Push(x any) {
	n := len(*pq)
	item := x.(*Item)
	item.index = n
	*pq = append(*pq, item)
}

func (pq *PQueue) Pop() any {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil  // Avoid memory leak
	item.index = -1 // For safety
	*pq = old[0 : n-1]

	return item
}

type Policies struct {
	LastRequestTime time.Time
	Delay           time.Duration
	mu              sync.Mutex
}

type Frontier struct {
	queue        *PQueue
	visited      map[string]struct{}
	policies     map[string]*Policies
	defaultDelay time.Duration
	mu           sync.Mutex
	cond         *sync.Cond
	closed       bool
}

func NewCrawlerFrontier(delay time.Duration) *Frontier {
	pq := make(PQueue, 0)
	heap.Init(&pq)
	f := &Frontier{
		queue:        &pq,
		visited:      make(map[string]struct{}),
		policies:     make(map[string]*Policies),
		defaultDelay: delay,
	}
	f.cond = sync.NewCond(&f.mu)
	return f
}

func (f *Frontier) Add(item *Item) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if _, seen := f.visited[item.URL]; seen {
		return
	}

	heap.Push(f.queue, item)
	f.visited[item.URL] = struct{}{}

	if _, exists := f.policies[item.Domain]; !exists {
		f.policies[item.Domain] = &Policies{
			Delay: f.defaultDelay,
		}
	}

	f.cond.Signal()
}

func (f *Frontier) Next() *Item {
	f.mu.Lock()
	defer f.mu.Unlock()

	for {
		if f.closed && f.queue.Len() == 0 {
			return nil
		}

		if f.queue.Len() > 0 {
			break
		}

		f.cond.Wait()
	}

	item := heap.Pop(f.queue).(*Item)
	policy := f.policies[item.Domain]
	policy.mu.Lock()
	defer policy.mu.Unlock()

	now := time.Now()
	elapsed := now.Sub(policy.LastRequestTime)
	wait := policy.Delay - elapsed
	if jitter := time.Duration(rand.Int63n(int64(policy.Delay/5))) - policy.Delay/10; wait+jitter > 0 {
		policy.mu.Unlock()
		time.Sleep(wait + jitter)
		policy.mu.Lock()
	}
	policy.LastRequestTime = time.Now()

	return item
}

func (f *Frontier) Close() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed = true
	f.cond.Broadcast()
}

type Crawler struct {
	launcherURL string
	Pool        rod.Pool[rod.Browser]
}

func InitCrawler() *Crawler {
	launch := launcher.New().
		Headless(true).
		UserDataDir("browser_data").
		Set("disable-web-security").
		Set("disable-notifications")

	url := launch.MustLaunch()
	Pool := rod.NewBrowserPool(WORKER_COUNT)
	c := Crawler{
		launcherURL: url,
		Pool:        Pool,
	}

	return &c
}

func (c *Crawler) GetBrowser() *rod.Browser {
	return c.Pool.MustGet(func() *rod.Browser {
		return rod.New().
			ControlURL(c.launcherURL).
			MustConnect()
	})
}

func (c *Crawler) GetTags(frontier *Frontier) {
	fmt.Println("Getting popular tags")

	page := c.GetBrowser().MustPage("https://dev.to/tags").MustWaitLoad()
	links := page.MustElements("a[href^='/t/']")
	for _, l := range links {
		relativeLink, err := l.Attribute("href")
		if err != nil || relativeLink == nil {
			log.Println(err)
			continue
		}

		absoluteURL, err := _normalizeURL("https://dev.to/", *relativeLink)
		if err != nil {
			log.Println(err)
			continue
		}

		if item, err := NewItem(absoluteURL, 0); err == nil {
			frontier.Add(item)
		}
	}
}

func ProcessTag(id int, item *Item, frontier *Frontier, browser *rod.Browser, writer *URLWriter) {
	page, err := browser.Page(proto.TargetCreateTarget{})
	if err != nil {
		fmt.Println(fmt.Errorf("Worker %d [depth %d] | error creating browser page: %v", id, item.Depth, err))
		return
	}
	defer page.MustClose()
	page.
		MustSetUserAgent(&proto.NetworkSetUserAgentOverride{UserAgent: PickUA()}).
		MustSetViewport(1920, 1080, 1, false)

	if err = rod.Try(func() { page.MustNavigate(item.URL).MustWaitLoad() }); err != nil {
		fmt.Println(fmt.Errorf("Worker %d [depth %d] | error navigating to page: %v", id, item.Depth, err))
		return
	}
	page.MustWaitIdle()

	infiniteScroll(page)
	links := page.MustElements("a[aria-labelledby]")
	for _, link := range links {
		attr, err := link.Attribute("href")
		if err != nil || attr == nil {
			continue
		}

		absoluteURL, err := _normalizeURL(item.URL, *attr)
		if err != nil {
			fmt.Println(err)
			continue
		}

		writer.Write(absoluteURL)
	}
}

// func ProcessURL(id int, item *Item, frontier *Frontier, browser *rod.Browser) {
// 	fmt.Printf("Worker %d [depth %d] at %s\n", id, item.Depth, item.URL)
//
// 	page, err := browser.Page(proto.TargetCreateTarget{})
// 	if err != nil {
// 		fmt.Println(fmt.Errorf("Worker %d [depth %d] | error creating browser page: %v", id, item.Depth, err))
// 		return
// 	}
// 	defer page.MustClose()
// 	page.
// 		MustSetUserAgent(&proto.NetworkSetUserAgentOverride{UserAgent: pickUA()}).
// 		MustSetViewport(1920, 1080, 1, false)
//
// 	if err = rod.Try(func() { page.MustNavigate(item.URL).MustWaitLoad() }); err != nil {
// 		fmt.Println(fmt.Errorf("Worker %d [depth %d] | error navigating to page: %v", id, item.Depth, err))
// 		return
// 	}
// 	page.MustWaitIdle()
//
// 	links := page.MustElements("a[aria-labelledby]")
// 	for _, link := range links {
// 		attr, err := link.Attribute("href")
// 		if err != nil || attr == nil {
// 			continue
// 		}
//
// 		if item.Depth < MAX_DEPTH {
// 			if newItem, err := NewItem(*attr, item.Depth+1); err == nil {
// 				frontier.Add(newItem)
// 			}
// 		}
// 	}
// }

func (c *Crawler) Cleanup() {
	c.Pool.Cleanup(func(p *rod.Browser) { p.MustClose() })
}

func infiniteScroll(page *rod.Page) {
	prevHeight := page.MustEval(`() => document.documentElement.scrollHeight`).Int()
	consecutiveEqualHeight := 0

	scrollCount := 0
	for scrollCount < 20 && consecutiveEqualHeight < 3 {
		page.Mouse.MustScroll(0, 15000)
		time.Sleep(1500 * time.Millisecond)
		page.MustWaitIdle()
		h := page.MustEval(`() => document.documentElement.scrollHeight`).Int()
		scrollCount++

		if h == prevHeight {
			consecutiveEqualHeight++
		} else {
			consecutiveEqualHeight = 0
			prevHeight = h
		}

		page.MustWaitRequestIdle()
		time.Sleep(200 * time.Millisecond)
	}
}

func _normalizeURL(base, href string) (string, error) {
	baseURL, err := url.Parse(base)
	if err != nil {
		return "", err
	}

	hrefURL, err := url.Parse(href)
	if err != nil {
		return "", err
	}

	resolved := baseURL.ResolveReference(hrefURL)
	resolved.Fragment = ""
	return resolved.String(), nil
}
