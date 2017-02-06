package main

import (
	"crypto/md5"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"time"

	"github.com/gorilla/mux"
	"github.com/hashicorp/consul/api"
)

var (
	client    *api.Client
	dataDir   = "./data"
	Namespace = "tuggle"
	Port      = 8080
	fetcherCh = make(chan fetchRequest, 10)
)

const (
	defaultContentType = "application/octet-stream"
	InternalHeader     = "X-Tuggle-Internal"
)

type Object struct {
	ID          string    `json:"id"`
	Name        string    `json:"name"`
	ContentType string    `json:"content_type"`
	Size        int64     `json:"size"`
	CreatedAt   time.Time `json:"created_at"`
}

func init() {
	var err error
	client, err = api.NewClient(api.DefaultConfig())
	if err != nil {
		panic(err)
	}
}

func main() {
	go fileFetcher()

	flag.StringVar(&dataDir, "data-dir", dataDir, "data directory")
	flag.IntVar(&Port, "port", Port, "listen port")
	flag.StringVar(&Namespace, "namespace", Namespace, "namespace")
	flag.Parse()

	m := mux.NewRouter()
	m.HandleFunc("/", indexHandler).Methods("GET")
	m.HandleFunc("/{name:[^/]+}", putHandler).Methods("PUT")
	m.HandleFunc("/{name:[^/]+}", getHandler).Methods("GET", "HEAD")

	log.Printf(
		"starting tuggle data-dir:%s port:%d namespace:%s\n",
		dataDir,
		Port,
		Namespace,
	)
	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", Port), m))
}

func registerService(obj *Object) error {
	var tags []string

	lock, err := client.LockKey(path.Join(Namespace, obj.ID+".lock"))
	if err != nil {
		return err
	}
	defer lock.Unlock()

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
	tags = append(tags, obj.ID)

	reg := &api.AgentServiceRegistration{
		ID:   Namespace,
		Name: Namespace,
		Tags: tags,
		Port: Port,
	}
	return client.Agent().ServiceRegister(reg)
}

func storeObject(obj *Object) error {
	kvp := &api.KVPair{
		Key: path.Join(Namespace, obj.Name),
	}
	kvp.Value, _ = json.Marshal(obj)

	wm, err := client.KV().Put(kvp, nil)
	if err != nil {
		return err
	}
	log.Printf("%s metadata stored to consul kv in %s", obj.Name, wm.RequestTime)
	return nil
}

func md5Hex(b []byte) string {
	h := md5.New()
	h.Write(b)
	return fmt.Sprintf("%x", h.Sum(nil))
}

func storeFile(name string, r io.ReadCloser) (*Object, error) {
	defer r.Close()
	filename := filepath.Join(dataDir, name)
	f, err := os.Create(filename)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	obj := Object{
		ID:        md5Hex([]byte(name)),
		Name:      name,
		CreatedAt: time.Now(),
	}

	if n, err := io.Copy(f, r); err != nil {
		return nil, err
	} else {
		obj.Size = n
	}
	return &obj, nil
}

func putHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	name := vars["name"]

	obj, err := storeFile(name, r.Body)
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

	w.Header().Set("Content-Type", "text/plain")
	w.WriteHeader(http.StatusCreated)
}

func indexHandler(w http.ResponseWriter, r *http.Request) {
	kv := client.KV()
	kvps, _, err := kv.List(Namespace, nil)
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
		r, err := loadFileOrRemote(req.Name)
		req.Ch <- fetchResponse{
			Reader: r,
			Err:    err,
		}
	}
}

func loadFileOrRemote(name string) (io.ReadCloser, error) {
	log.Println("load", name, "from local")
	f, err := loadFile(name)

	if f != nil && err == nil {
		log.Println("hit local file")
		// hit local file
		return f, nil
	}

	// lookup service catlog
	catalogServices, _, err := client.Catalog().Service(
		Namespace,            // service name
		md5Hex([]byte(name)), // tag is md5 hex
		&api.QueryOptions{
			RequireConsistent: true,
			Near:              "_agent",
		},
	)
	if err != nil {
		return nil, err
	}
	if len(catalogServices) == 0 {
		return nil, errors.New("Not Found in this cluster")
	}

	for _, cs := range catalogServices {
		u := fmt.Sprintf(
			"http://%s:%d/%s",
			cs.Address,
			cs.ServicePort,
			name,
		)
		log.Println("fetching from", u)
		req, _ := http.NewRequest("GET", u, nil)
		req.Header.Set(InternalHeader, "True")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			log.Println(err)
			continue
		}
		if resp.StatusCode != http.StatusOK {
			continue
		}

		obj, err := storeFile(name, resp.Body)
		if err != nil {
			log.Println(err)
			continue
		}
		if err := registerService(obj); err != nil {
			log.Println(err)
			continue
		}
		return loadFile(name)
	}
	return nil, errors.New("Not found in tuggle cluster")
}

func getHandler(w http.ResponseWriter, r *http.Request) {
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
	w.Header().Set("Content-Type", obj.ContentType)
	w.Header().Set("Last-Modified", obj.CreatedAt.Format(time.RFC1123))
	w.Header().Set("X-Tuggle-Object-ID", obj.ID)

	if r.Method == http.MethodHead {
		w.Header().Set("Content-Length", strconv.FormatInt(obj.Size, 10))
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

	w.Header().Set("Content-Length", strconv.FormatInt(obj.Size, 10))
	io.Copy(w, f)
}
