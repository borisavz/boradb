package main

import (
	"context"
	"errors"
	"github.com/gorilla/mux"
	"github.com/huandu/skiplist"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

type Engine struct {
	currentMemtable     *skiplist.SkipList
	readOnlyMemtable    *skiplist.SkipList
	currentMemtableLock sync.RWMutex
	compactionLock      sync.Mutex
}

type MemtableEntry struct {
	value     string
	tombstone bool
}

func InitializeEngine() *Engine {
	list1 := skiplist.New(skiplist.StringAsc)

	list1.Set("a", MemtableEntry{"val a", false})
	list1.Set("b", MemtableEntry{"val b", false})

	return &Engine{
		currentMemtable: list1,
	}
}

func (e *Engine) GetValue(key string) (string, error) {
	e.compactionLock.Lock()
	e.currentMemtableLock.RLock()

	defer e.currentMemtableLock.RUnlock()
	defer e.compactionLock.Unlock()

	val, ok := e.currentMemtable.GetValue(key)
	if !ok {
		val, ok = e.readOnlyMemtable.GetValue(key)
		if !ok {
			return "", errors.New("Key does not exist!")
		}

		entry := val.(MemtableEntry)

		if entry.tombstone {
			return "", errors.New("Key does not exist!")
		}

		return entry.value, nil
	}

	entry := val.(MemtableEntry)

	if entry.tombstone {
		return "", errors.New("Key does not exist!")
	}

	return entry.value, nil
}

func (e *Engine) PutValue(key string, value string) error {
	e.compactionLock.Lock()
	e.currentMemtableLock.Lock()

	entry := MemtableEntry{value, false}

	e.currentMemtable.Set(key, entry)

	e.compactionLock.Unlock()
	e.currentMemtableLock.Unlock()

	if e.currentMemtable.Len() == 3 {
		e.triggerBackgroundCompaction()
	}

	return nil
}

func (e *Engine) DeleteValue(key string) error {
	e.compactionLock.Lock()
	e.currentMemtableLock.Lock()

	deletedEntry := MemtableEntry{"", true}

	e.currentMemtable.Set(key, deletedEntry)

	e.compactionLock.Unlock()
	e.currentMemtableLock.Unlock()

	return nil
}

func (e *Engine) triggerBackgroundCompaction() {
	e.compactionLock.Lock()

	e.readOnlyMemtable = e.currentMemtable
	e.currentMemtable = skiplist.New(skiplist.StringAsc)

	e.compactionLock.Unlock()
}

var engine *Engine

func main() {
	engine = InitializeEngine()
	InitializeHTTP()
}

func InitializeHTTP() {
	quit := make(chan os.Signal)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)

	router := mux.NewRouter()

	router.HandleFunc("/{key}", GetValue).Methods("GET")
	router.HandleFunc("/{key}", PutValue).Methods("PUT")
	router.HandleFunc("/{key}", DeleteValue).Methods("DELETE")

	// start server
	srv := &http.Server{
		Addr:    "0.0.0.0:8000",
		Handler: router,
	}

	go func() {
		log.Println("server starting")

		if err := srv.ListenAndServe(); err != nil {
			if err != http.ErrServerClosed {
				log.Fatal(err)
			}
		}
	}()

	<-quit

	log.Println("service shutting down ...")

	// gracefully stop server
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		log.Fatal(err)
	}
	log.Println("server stopped")
}

func GetValue(w http.ResponseWriter, req *http.Request) {
	key := mux.Vars(req)["key"]

	value, err := engine.GetValue(key)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Write([]byte(value))
}

func PutValue(w http.ResponseWriter, req *http.Request) {
	key := mux.Vars(req)["key"]

	bodyBytes, err := ioutil.ReadAll(req.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	bodyString := string(bodyBytes)

	err = engine.PutValue(key, bodyString)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func DeleteValue(w http.ResponseWriter, req *http.Request) {
	key := mux.Vars(req)["key"]

	err := engine.DeleteValue(key)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}
