package main

import (
	"context"
	"github.com/gorilla/mux"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

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
