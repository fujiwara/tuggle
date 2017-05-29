package main

import (
	"context"
	"crypto/md5"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"mime"
	"net/http"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"text/template"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/fujiwara/shapeio"
	"github.com/gorilla/mux"
	"github.com/hashicorp/consul/api"
)

var (
	mu                sync.Mutex
	client            *api.Client
	dataDir           = "./data"
	Namespace         = "tuggle"
	Port              = 8900
	isSlave           = false
	MaxFetchMultiplex = 3
	MaxFetchRate      float64
	MaxFetchDelay     = 10 * time.Second
	FetchTimeout      = 600 * time.Second
	fetcherCh         = make(chan fetchRequest, 10)
	graphTemplate     = `
digraph "{{.Name}}" {
  graph [
    charset = "UTF-8",
    label   = "{{.Name}}",
    rankdir = LR
  ];
  node [
    shape = box
  ];
  edge [
    color = gray,
    fontsize = 12
  ];
{{range .Nodes}}
  "{{.From}}" -> "{{.To}}" [
    headlabel = "{{formatTime .End}}",
    label     = "{{formatDuration .Elapsed}}"
  ];
{{end}}
}
`
	graphTmpl *template.Template
)

const (
	defaultContentType = "application/octet-stream"
	InternalHeader     = "X-Tuggle-Internal"
	MethodFetch        = "FETCH"
	MethodDelete       = "DELETE"
)

type Object struct {
	ID          string    `json:"id"`
	Name        string    `json:"name"`
	ContentType string    `json:"content_type"`
	Size        int64     `json:"size"`
	CreatedAt   time.Time `json:"created_at"`
}

type Graph struct {
	From    string        `json:"from"`
	To      string        `json:"to"`
	Start   time.Time     `json:"start"`
	End     time.Time     `json:"end"`
	Elapsed time.Duration `json:"elapsed"`
}

type Graphs []*Graph

func (gs Graphs) Len() int {
	return len(gs)
}

func (gs Graphs) Less(i, j int) bool {
	return gs[i].Start.Before(gs[j].Start)
}

func (gs Graphs) Swap(i, j int) {
	gs[i], gs[j] = gs[j], gs[i]
}

func NewGraph(from, to string, start time.Time) *Graph {
	now := time.Now()
	return &Graph{
		From:    from,
		To:      to,
		Start:   start,
		End:     now,
		Elapsed: now.Sub(start),
	}
}

func md5Hex(b []byte) string {
	h := md5.New()
	h.Write(b)
	return fmt.Sprintf("%x", h.Sum(nil))
}

func NewObject(name string) *Object {
	return &Object{
		ID:        md5Hex([]byte(name)),
		Name:      name,
		CreatedAt: time.Now(),
	}
}

func init() {
	var err error
	client, err = api.NewClient(api.DefaultConfig())
	if err != nil {
		panic(err)
	}

	funcs := template.FuncMap{
		"formatTime": func(t *time.Time) string {
			return t.Format("15:04:05")
		},
		"formatDuration": func(d *time.Duration) string {
			return fmt.Sprintf("%.2fs", d.Seconds())
		},
	}
	graphTmpl, err = template.New("graph").Funcs(funcs).Parse(graphTemplate)
	if err != nil {
		panic(err)
	}

	rand.Seed(time.Now().UnixNano())
}

func main() {
	go fileFetcher()
	go eventWatcher()

	var (
		fetchRate    string
		fetchTimeout string
	)
	flag.StringVar(&dataDir, "data-dir", dataDir, "data directory")
	flag.IntVar(&Port, "port", Port, "listen port")
	flag.StringVar(&Namespace, "namespace", Namespace, "namespace")
	flag.StringVar(&fetchRate, "fetch-rate", "unlimited", "Max fetch rate limit(/sec)")
	flag.StringVar(&fetchTimeout, "fetch-timeout", FetchTimeout.String(), "fetch timeout")
	flag.BoolVar(&isSlave, "slave", false, "slave mode (fetch only)")
	flag.Parse()

	if fetchRate != "unlimited" && fetchRate != "" {
		if rate, err := humanize.ParseBytes(fetchRate); err != nil {
			fmt.Println("Cannot parse -fetch-rate", err)
			os.Exit(1)
		} else {
			MaxFetchRate = float64(rate)
		}
	}
	if fetchTimeout != "" {
		d, err := time.ParseDuration(fetchTimeout)
		if err != nil {
			fmt.Println("Cannot parse -fetch-timeout", err)
			os.Exit(1)
		}
		FetchTimeout = d
	}

	m := mux.NewRouter()
	m.HandleFunc("/", indexHandler).Methods("GET")
	m.HandleFunc("/{name:[^/]+}", putHandler).Methods("PUT")
	m.HandleFunc("/{name:[^/]+}", getHandler).Methods("GET", "HEAD")
	m.HandleFunc("/{name:[^/]+}", deleteHandler).Methods("DELETE")

	slaveMsg := ""
	if isSlave {
		slaveMsg = " slave mode"
	}
	log.Printf(
		"starting tuggle data-dir:%s port:%d namespace:%s fetch-rate:%s/sec%s",
		dataDir,
		Port,
		Namespace,
		fetchRate,
		slaveMsg,
	)
	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", Port), m))
}

func manageService(obj *Object, register bool) error {
	mu.Lock()
	defer mu.Unlock()

	var tags []string
	sv, err := client.Agent().Services()
	if err != nil {
		return err
	}
	if service := sv[Namespace]; service != nil {
		for _, tag := range service.Tags {
			if tag != obj.ID {
				tags = append(tags, tag)
			}
		}
	}
	if register {
		tags = append(tags, obj.ID)
	}

	reg := &api.AgentServiceRegistration{
		ID:   Namespace,
		Name: Namespace,
		Tags: tags,
		Port: Port,
	}
	return client.Agent().ServiceRegister(reg)
}

func registerService(obj *Object) error {
	return manageService(obj, true)
}

func deregisterService(obj *Object) error {
	return manageService(obj, false)
}

func eventWatcher() {
	var lastIndex uint64
	processedEvents := make(map[string]bool)
WATCH:
	for {
		events, qm, err := client.Event().List(
			Namespace, // eventName
			&api.QueryOptions{
				WaitIndex: lastIndex,
				WaitTime:  time.Second * 10,
			},
		)
		if err != nil {
			log.Println(err)
			time.Sleep(time.Second)
			continue WATCH
		}
	EVENT:
		for _, ev := range events {
			if processedEvents[ev.ID] {
				continue EVENT
			}
			processedEvents[ev.ID] = true
			if lastIndex == 0 {
				// at first time, ignore all stucked events
				continue EVENT
			}
			err := processEvent(ev.Payload)
			if err != nil {
				log.Println(err)
			}
		}
		lastIndex = qm.LastIndex
	}
}

func newPayloadFetch(name string) []byte {
	return []byte(MethodFetch + ":" + name)
}

func newPayloadDelete(name string) []byte {
	return []byte(MethodDelete + ":" + name)
}

func parsePayload(payload []byte) (method, name string, err error) {
	p := strings.SplitN(string(payload), ":", 2)
	if len(p) != 2 {
		return "", "", fmt.Errorf("invalid payload %s", string(payload))
	}
	return p[0], p[1], nil
}

func processEvent(payload []byte) error {
	method, name, err := parsePayload(payload)
	if err != nil {
		return err
	}
	switch method {
	case MethodFetch:
		log.Println("fetching", name)
		f, err := fetch(name)
		if err != nil {
			return err
		}
		f.Close()
	case MethodDelete:
		log.Println("deleting", name)
		obj := NewObject(name)
		if err := deregisterService(obj); err != nil {
			return err
		}
		if err := purgeFile(name); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unknown payload %s", payload)
	}
	return nil
}

func recordGraph(name string, g *Graph) error {
	kvp := &api.KVPair{
		Key: path.Join(Namespace+".graph", name, g.From, g.To),
	}
	kvp.Value, _ = json.Marshal(g)

	_, err := client.KV().Put(kvp, nil)
	if err != nil {
		return err
	}
	return nil
}

func deleteGraphTree(name string) error {
	key := path.Join(Namespace+".graph", name)
	_, err := client.KV().DeleteTree(key+"/", nil)
	if err != nil {
		return err
	}
	return nil
}

func storeObject(obj *Object) error {
	kvp := &api.KVPair{
		Key: path.Join(Namespace, obj.Name),
	}
	kvp.Value, _ = json.Marshal(obj)

	_, err := client.KV().Put(kvp, nil)
	if err != nil {
		return err
	}
	return nil
}

func purgeObject(obj *Object) error {
	_, err := client.KV().Delete(
		path.Join(Namespace, obj.Name),
		nil,
	)
	return err
}

func purgeFile(name string) error {
	return os.Remove(filepath.Join(dataDir, name))
}

func storeFile(name string, r io.ReadCloser) (*Object, error) {
	defer r.Close()
	i := rand.Int()
	filename := filepath.Join(dataDir, name)
	tmp := filename + "." + strconv.Itoa(i)
	f, err := os.Create(tmp)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	obj := NewObject(name)

	if n, err := io.Copy(f, r); err != nil {
		return nil, err
	} else {
		obj.Size = n
	}
	if err := os.Rename(tmp, filename); err != nil {
		return nil, err
	}
	return obj, nil
}

func putHandler(w http.ResponseWriter, r *http.Request) {
	if isSlave {
		http.Error(w, "slave mode", http.StatusMethodNotAllowed)
		return
	}

	vars := mux.Vars(r)
	name := vars["name"]

	obj, err := loadObject(name)
	if err != nil {
		log.Println(err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if obj != nil {
		http.Error(w, "exists", http.StatusMethodNotAllowed)
		return
	}

	obj, err = storeFile(name, r.Body)
	if err != nil {
		log.Println(err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if t := r.Header.Get("Content-Type"); t != "" {
		obj.ContentType = t
	} else {
		obj.ContentType = defaultContentType
	}

	if err := storeObject(obj); err != nil {
		log.Println(err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if err := registerService(obj); err != nil {
		log.Println(err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	r.ParseForm() // parse must do after r.Body is read.
	if len(r.Form["sync"]) > 0 {
		p := newPayloadFetch(name)
		ev := &api.UserEvent{
			Name:    Namespace,
			Payload: p,
		}
		eventID, _, err := client.Event().Fire(ev, nil)
		if err != nil {
			log.Println(err)
		} else {
			log.Printf("event %s fired ID:%s", string(p), eventID)
		}
	}

	w.Header().Set("Content-Type", "text/plain")
	w.WriteHeader(http.StatusCreated)
}

func lockFetchMultiplex(key string) (*api.Semaphore, error) {
	prefix := path.Join(Namespace+".lock", key)
	sem, _ := client.SemaphoreOpts(&api.SemaphoreOptions{
		Prefix:            prefix,
		Limit:             MaxFetchMultiplex,
		SemaphoreWaitTime: time.Second,
		SemaphoreTryOnce:  true,
	})
	ch, err := sem.Acquire(nil)
	if err != nil {
		return nil, err
	}
	if ch == nil {
		sem.Release()
		return nil, nil
	}
	return sem, nil
}

func indexHandler(w http.ResponseWriter, r *http.Request) {
	kv := client.KV()
	kvps, _, err := kv.List(Namespace+"/", nil)
	if err != nil {
		log.Println(err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	objects := make([]Object, 0, len(kvps))
	for _, kvp := range kvps {
		var o Object
		if err := json.Unmarshal(kvp.Value, &o); err != nil {
			log.Println(err)
			continue
		}
		objects = append(objects, o)
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(objects)
}

func loadObject(name string) (*Object, error) {
	kv := client.KV()
	kvp, _, err := kv.Get(path.Join(Namespace, name), nil)
	if err != nil {
		return nil, err
	}
	if kvp == nil {
		return nil, nil
	}
	var obj Object
	if err := json.Unmarshal(kvp.Value, &obj); err != nil {
		return nil, err
	}
	return &obj, nil
}

func loadFile(name string) (io.ReadCloser, error) {
	return os.Open(filepath.Join(dataDir, name))
}

type fetchResponse struct {
	Reader io.ReadCloser
	Err    error
}

type fetchRequest struct {
	Name string
	Ch   chan fetchResponse
}

func fetch(name string) (io.ReadCloser, error) {
	ch := make(chan fetchResponse, 1)
	fetcherCh <- fetchRequest{
		Name: name,
		Ch:   ch,
	}
	res := <-ch
	return res.Reader, res.Err
}

func fileFetcher() {
	for req := range fetcherCh {
		if isSlave {
			// waits a little because slave will not become supplier
			time.Sleep(1 * time.Second)
		}
		r, err := loadFileOrRemote(req.Name)
		req.Ch <- fetchResponse{
			Reader: r,
			Err:    err,
		}
	}
}

func loadFileOrRemote(name string) (io.ReadCloser, error) {
	f, err := loadFile(name)
	if err == nil && f != nil {
		log.Println("hit local file")
		return f, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), FetchTimeout)
	defer cancel()
	i := 0
	for {
		i++
		// lookup service catlog
		catalogServices, _, err := client.Catalog().Service(
			Namespace,            // service name
			md5Hex([]byte(name)), // tag is Object.ID
			&api.QueryOptions{
				RequireConsistent: true,
				Near:              "_agent",
			},
		)
		if err != nil {
			log.Println(err)
			continue
		}
		nodes := len(catalogServices)
		if nodes == 0 {
			return nil, errors.New("Not Found in this cluster")
		}

		//pick up randomly from nearest 3 nodes
		n := 3
		if nodes < n {
			n = nodes
		}
		cs := catalogServices[rand.Intn(n)]

		sem, err := lockFetchMultiplex(cs.Address)
		if err != nil || sem == nil {
			select {
			case <-ctx.Done():
				return nil, fmt.Errorf("timed out")
			default:
			}
			d := float64(i) + rand.ExpFloat64()
			delay, _ := time.ParseDuration(fmt.Sprintf("%0.9fs", d))
			if delay > MaxFetchDelay {
				delay = MaxFetchDelay
			}
			log.Printf(
				"failed to get semaphore for %s waiting %s",
				cs.Address,
				delay,
			)
			time.Sleep(delay)
			continue
		}
		start := time.Now()
		err = loadRemoteAndStore(
			ctx,
			fmt.Sprintf("%s:%d", cs.Address, cs.ServicePort),
			name,
		)
		sem.Release()
		if err != nil {
			log.Println(err)
			select {
			case <-ctx.Done():
				return nil, errors.New("fetch timed out")
			default:
			}
			continue
		}
		self, _ := client.Agent().NodeName()
		err = recordGraph(
			name,
			NewGraph(cs.Node, self, start),
		)
		if err != nil {
			log.Println(err)
		}
		return loadFile(name)
	}
	return nil, fmt.Errorf("failed to get %s in this cluster", name)
}

type LimitedReader struct {
	*shapeio.Reader
	src io.ReadCloser
}

func (r *LimitedReader) Close() error {
	return r.src.Close()
}

func NewLimitedReader(src io.ReadCloser, limit float64) *LimitedReader {
	reader := shapeio.NewReader(src)
	if limit != 0 {
		reader.SetRateLimit(limit)
	}
	return &LimitedReader{reader, src}
}

func loadRemoteAndStore(ctx context.Context, addr, name string) error {
	u := fmt.Sprintf("http://%s/%s", addr, name)
	log.Printf("loading remote %s", u)
	req, _ := http.NewRequest("GET", u, nil)
	req.Header.Set(InternalHeader, "True")

	start := time.Now()
	resp, err := http.DefaultClient.Do(req.WithContext(ctx))
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return err
	}

	reader := NewLimitedReader(resp.Body, MaxFetchRate)
	obj, err := storeFile(name, reader)
	if err != nil {
		return err
	}
	elapsed := time.Since(start)
	log.Printf("%s fetched %s bytes in %.2f sec (%s/sec)",
		name,
		humanize.Comma(obj.Size),
		elapsed.Seconds(),
		humanize.Bytes(uint64(float64(obj.Size)/elapsed.Seconds())),
	)
	if !isSlave {
		if err := registerService(obj); err != nil {
			return err
		}
	}
	return nil
}

func getHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	name := vars["name"]
	r.ParseForm()

	obj, err := loadObject(name)
	if err != nil {
		log.Println(err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if obj == nil {
		http.Error(w, "Not Found", http.StatusNotFound)
		return
	}

	if len(r.Form["graph"]) > 0 {
		graphHandler(w, r)
		return
	}

	w.Header().Set("Content-Type", obj.ContentType)
	w.Header().Set("Content-Length", strconv.FormatInt(obj.Size, 10))
	w.Header().Set("Last-Modified", obj.CreatedAt.Format(time.RFC1123))
	w.Header().Set("X-Tuggle-Object-ID", obj.ID)

	if r.Method == http.MethodHead {
		return
	}

	// default action
	action := fetch

	// internal access won't fallback to remote cluster
	if r.Header.Get(InternalHeader) != "" {
		action = loadFile
	}

	f, err := action(name)
	if err != nil {
		log.Println(err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer f.Close()

	io.Copy(w, f)
}

func graphHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	name := vars["name"]

	kv := client.KV()
	key := path.Join(Namespace+".graph", name)
	kvps, _, err := kv.List(key+"/", nil)
	if err != nil {
		log.Println(err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	gs := make(Graphs, 0, len(kvps))
	for _, kvp := range kvps {
		var g Graph
		if err := json.Unmarshal(kvp.Value, &g); err != nil {
			log.Println(err)
			continue
		}
		gs = append(gs, &g)
	}
	sort.Sort(gs)
	v := struct {
		Name  string
		Nodes []*Graph
	}{
		Name:  name,
		Nodes: gs,
	}

	var contentType string
	format := r.FormValue("format")
	switch format {
	case "png", "gif", "svg", "svgx":
		contentType = mime.TypeByExtension("." + format)
	default:
		contentType = "text/vnd.graphviz"
	}
	w.Header().Set("Content-Type", contentType)
	w.WriteHeader(http.StatusOK)

	if contentType != "text/vnd.graphviz" {
		cmd := exec.Command("dot", "-T"+format)
		cmd.Stderr = os.Stderr
		cmd.Stdout = w
		p, _ := cmd.StdinPipe()
		if err := cmd.Start(); err != nil {
			log.Println(err)
			return
		}
		graphTmpl.Execute(p, v)
		p.Close()
		if err := cmd.Wait(); err != nil {
			log.Println(err)
			return
		}
	} else {
		graphTmpl.Execute(w, v)
	}
}

func deleteHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	name := vars["name"]

	obj, err := loadObject(name)
	if err != nil {
		log.Println(err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if obj == nil {
		http.Error(w, "Not Found", http.StatusNotFound)
		return
	}

	if err := purgeObject(obj); err != nil {
		log.Println(err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}

	if err := deleteGraphTree(obj.Name); err != nil {
		log.Println(err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}

	p := newPayloadDelete(obj.Name)
	ev := &api.UserEvent{
		Name:    Namespace,
		Payload: p,
	}
	eventID, _, err := client.Event().Fire(ev, nil)
	if err != nil {
		log.Println(err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
	log.Printf("event %s fired ID:%s", string(p), eventID)
	return
}
