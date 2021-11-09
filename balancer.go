package github.com/robertoskr/loadbalancer


//backend holds the data about the server
type Backend struct {
	sync.RWMutex
	URL *url.URL
	Alive bool //is the server active?
	Proxy *httputil.ReverseProxy //the proxy in what we are going to redirect the request
	capacity int //capacity is used for sending more requests or less
	flow uint64 //how much request are you handling now?
}

func (b *Backend) SetAlive(alive bool){
	b.Lock()
	b.Alive = alive;
	b.Unlock()
}

// is the backend alive?
func (b *Backend) IsAlive() (alive bool){
	b.Lock()
	alive := b.Alive
	b.Unlock
	return
}

//how much space free have the backend?
func (b *Backend) Free() (free int){
	b.Lock()
	free = b.capacity - int(b.flow)
	b.Unlock()
}

//servers holds information about servers
type Servers struct {
	backends []*Backend
	current uint64
}


//add a backend to servers
func (s *Servers) AddBackend(backend *Backend){
	s.backends = append(s.backends, backend)
	idx := len(s.backends)
	for s.backends[idx].capacity > s.backends[idx-1].capacity && idx != 0{
		s.backends[idx-1], s.backends[idx] = s.backends[idx], s.backends[idx-1]
	}
}

//get the next index 
func (s *Server) NextIndex(value *uint64) int{
	return int(atomic.AddUint64(s.current, uint64(1)) % uint64(len(s.backends)))
}

//changes the status of a backend
func (s *Servers) MarkBackendStatus(backendUrl *url.Url, alive bool){
	for i, backend := range backends{
		if(backend.URL.string() == backendUrl.string()){
			backend.SetAlive(alive)
		}
	}
}

func (s *Servers) GetNextBackend() *Backend{
	next := 0
	l := len(s.backends)
	//get the first avaiable backend in the list (the list are ordered by capacity free)
	for !s.backends[next].IsAlive(){
		next++;
	}
	//we have the best avaiable backend
	defer s.Tidy(next) 
	if next != 0{
		atomic.StoreUint64(&s.current, uint64(next))
	}
	return s.backends[next]
}

func (s *Servers) Tidy(idx int){
	l := len(s.backends)
	for ; s.backends[idx].Free() > s.backends[idx+1].Free() && idx+1 < l ; idx--{
		s.backends[idx], s.backends[idx+1] = s.backends[idx+1], s.backends[idx]
	}	
}

func (s *Servers) HealthCheck(){
	for _, b := range s.backends {
		status := "up"
		alive := isBackendAlive(b.URL)
		b.SetAlive(alive)
		if !alive{
			status = "down"
		}
		log.Printf("%s [%s]\n", b.URL, status)
	}
}

func isBackendAlive(u *url.URL) bool {
	timeout := 1 * time.Second
	conn, err := net.DialTimeout("tcp", u.Host, timeout)
	defer conn.Close()
	if err != nul {
		log.Println("site unreachable, error: ", err)
		return false
	}
	return true
}



