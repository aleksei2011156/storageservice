package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/gorilla/mux"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Object struct {
	Key     string
	Value   json.RawMessage
	Expires *time.Time
}

type Storage struct {
	mu      sync.RWMutex
	objects map[string]Object
}

type App struct {
	storage  *Storage
	filename string
}

func NewStorage() *Storage {
	return &Storage{
		objects: make(map[string]Object),
	}
}

func NewApp(filename string) *App {
	return &App{
		storage:  NewStorage(),
		filename: filename,
	}
}

func (s *Storage) WriteObject(key string, obj Object) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.objects[key] = obj
}

func (s *Storage) ReadObject(key string) (Object, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	obj, ok := s.objects[key]
	return obj, ok
}

func (s *Storage) DeleteObject(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.objects, key)
}

func (a *App) LoadFromFile() error {
	file, err := ioutil.ReadFile(a.filename)
	if err != nil {
		return err
	}

	var objects map[string]Object

	err = json.Unmarshal(file, &objects)
	if err != nil {
		return err
	}
	a.storage.mu.Lock()
	defer a.storage.mu.Unlock()
	a.storage.objects = objects
	return nil
}

func (a *App) SaveToFile() error {
	a.storage.mu.RLock()
	defer a.storage.mu.RUnlock()

	file, err := json.Marshal(a.storage.objects)
	if err != nil {
		return err
	}

	err = ioutil.WriteFile(a.filename, file, 0644)
	if err != nil {
		return err
	}
	return nil
}

func (a *App) WriteObjectHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := vars["key"]

	var obj Object
	err := json.NewDecoder(r.Body).Decode(&obj)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	expireStr := r.Header.Get("Expires")
	if expireStr != "" {
		expires, err := time.Parse(time.RFC3339, expireStr)
		if err != nil {
			http.Error(w, "Invalid Expires header format", http.StatusBadRequest)
			return
		}
		obj.Expires = &expires

	}
	a.storage.WriteObject(key, obj)
	w.WriteHeader(http.StatusCreated)

}

func (a *App) ReadObjectHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := vars["key"]

	obj, ok := a.storage.ReadObject(key)
	if !ok {
		http.NotFound(w, r)
		return
	}
	json.NewEncoder(w).Encode(obj)
}

func LivenessHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
}

func ReadinessHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
}

func main() {
	sourcejson, error := os.Getwd()
	if error == nil {
		sourcejson += "/storage.json"
	}

	app := NewApp(sourcejson)
	err := app.LoadFromFile()
	if err != nil {
		fmt.Println("Error loading state from file: ", err)
	}

	prometheusHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		promhttp.Handler().ServeHTTP(w, r)
	})
	r := mux.NewRouter()

	r.HandleFunc("/objects/{key}", app.WriteObjectHandler).Methods("PUT")
	r.HandleFunc("/objects/{key}", app.ReadObjectHandler).Methods("GET")
	r.HandleFunc("/probes/liveness", LivenessHandler).Methods("GET")
	r.HandleFunc("/probes/readiness", ReadinessHandler).Methods("GET")
	r.HandleFunc("/metrics", prometheusHandler)

	go func() {
		for {
			time.Sleep(30 * time.Second)
			err := app.SaveToFile()
			if err != nil {
				fmt.Println("Error saving state to file: ", err)
			}
		}
	}()

	http.Handle("/", r)
	http.ListenAndServe(":8080", nil)
}
