package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"sync"
	"sync/atomic"
	"time"
)

//backend holds the data about the server
type Backend struct {
	sync.RWMutex
	URL      *url.URL
	Alive    bool                   //is the server active?
	Proxy    *httputil.ReverseProxy //the proxy in what we are going to redirect the request
	capacity int                    //capacity is used for sending more requests or less
	flow     uint64                 //how much request are you handling now?
}

func (b *Backend) SetAlive(alive bool) {
	b.Lock()
	b.Alive = alive
	b.Unlock()
}

// is the backend alive?
func (b *Backend) IsAlive() (alive bool) {
	b.Lock()
	alive = b.Alive
	b.Unlock()
	return
}

//how much space free have the backend?
func (b *Backend) Free() (free int) {
	b.Lock()
	free = b.capacity - int(b.flow)
	b.Unlock()
	return
}

//servers holds information about servers
type Servers struct {
	backends []*Backend
	current  uint64
}

//add a backend to servers
func (s *Servers) AddBackend(backend *Backend) {
	s.backends = append(s.backends, backend)
	idx := len(s.backends)
	for s.backends[idx].capacity > s.backends[idx-1].capacity && idx != 0 {
		s.backends[idx-1], s.backends[idx] = s.backends[idx], s.backends[idx-1]
	}
}

//get the next index
func (s *Servers) NextIndex(value *uint64) int {
	return int(atomic.AddUint64(&s.current, uint64(1)) % uint64(len(s.backends)))
}

//changes the status of a backend
func (s *Servers) MarkBackendStatus(backendUrl *url.URL, alive bool) {
	for _, backend := range s.backends {
		if backend.URL.String() == backendUrl.String() {
			backend.SetAlive(alive)
		}
	}
}

func (s *Servers) GetNextBackend() *Backend {
	next := 0
	//get the first avaiable backend in the list (the list are ordered by capacity free)
	for !s.backends[next].IsAlive() {
		next++
	}
	//we have the best avaiable backend
	defer s.Tidy(next)
	if next != 0 {
		atomic.StoreUint64(&s.current, uint64(next))
	}
	return s.backends[next]
}

func (s *Servers) Tidy(idx int) {
	l := len(s.backends)
	for ; s.backends[idx].Free() > s.backends[idx+1].Free() && idx+1 < l; idx-- {
		s.backends[idx], s.backends[idx+1] = s.backends[idx+1], s.backends[idx]
	}
}

func (s *Servers) HealthCheck() {
	for _, b := range s.backends {
		status := "up"
		alive := isBackendAlive(b.URL)
		b.SetAlive(alive)
		if !alive {
			status = "down"
		}
		log.Printf("%s [%s]\n", b.URL, status)
	}
}

func isBackendAlive(u *url.URL) bool {
	timeout := 1 * time.Second
	conn, err := net.DialTimeout("tcp", u.Host, timeout)
	defer conn.Close()
	if err != nil {
		log.Println("site unreachable, error: ", err)
		return false
	}
	return true
}

//get attempts returns the attempts for request
func GetAttemptsFromContext(r *http.Request) int {
	if attempts, ok := r.Context().Value("Attempts").(int); ok {
		return attempts
	}
	return 1
}

func GetRetryFromContext(r *http.Request) int {
	if retries, ok := r.Context().Value("Retry").(int); ok {
		return retries
	}
	return 0
}

//this is the load balancer that balances the load of the server
func lb(w http.ResponseWriter, r *http.Request) {
	attempts := GetAttemptsFromContext(r)
	if attempts > 3 {
		log.Printf("%s(%s) Max attempts reached, termitating service", r.RemoteAddr, r.URL.Path)
		http.Error(w, "service not avaiable", http.StatusServiceUnavailable)
		return
	}
	backend := servers.GetNextBackend()
	if backend != nil {
		backend.Proxy.ServeHTTP(w, r)
		fmt.Println(servers.backends)
		return
	}
	http.Error(w, "service not avaiable", http.StatusServiceUnavailable)
}

var servers Servers

func DefaultErrorHandler(serverUrl url.URL) func() (http.ResponseWriter, http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		log.Printf("[%s] %s\n", serverUrl.Host, e.Error())
		retries := GetRetryFromContext(r)
		if retries < 3 {
			//if retries are less than 3 wee can try it again
			select {
			case <-time.After(10 * time.Millisecond):
				ctx := context.WithValue(request.Context(), Retry, retries)
				proxy.ServeHTTP(r, request.WithContext(ctx))
			}
			return
		}
		servers.MarckBackendStatus(serverUrl, false)
		// if the same request routing for few attempts with different backends, increase the count
		attempts := GetAttemptsFromContext(request)
		log.Printf("%s(%s) Attempting retry %d\n", request.RemoteAddr, request.URL.Path, attempts)
		ctx := context.WithValue(request.Context(), Attempts, attempts+1)
		lb(writer, request.WithContext(ctx))
	}
}

func main() {
	//var serverList string
	//var port int
	//reader := bufio.NewReader(os.Stdin)
	fmt.Println("how much servers do you want to use?")
	var nservers int
	ports := make([]string, 0)
	var port string
	_, err := fmt.Scanf("%d", &nservers)
	for ; nservers != 0 && err == nil; nservers-- {
		fmt.Scanf("%s\n", &port)
		ports = append(ports, port)
	}

	for uri := range ports {
		serverURL, err := url.Parse(uri)
		if err != nil {
			log.Fatal(err)
		}

		proxy := httputil.NewSingleHostReverseProxy(serverURL)
		proxy.ErrorHandler = DefaultErrorHandler(serverURL)

		servers.AddBackend(&Backend{
			URL:   serverURL,
			Alive: true,
			Proxy: proxy,
		})
		log.Printf("Configured server: %s\n", serverURL)
	}

	// create http server
	server := http.Server{
		Addr:    fmt.Sprintf(":%d", port),
		Handler: http.HandlerFunc(lb),
	}

	// start health checking
	go healthCheck()

	log.Printf("Load Balancer started at :%d\n", port)
	if err := server.ListenAndServe(); err != nil {
		log.Fatal(err)
	}
}
