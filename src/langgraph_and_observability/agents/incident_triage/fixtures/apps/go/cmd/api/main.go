package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"golang.org/x/sync/errgroup"

	"github.com/example/orders-api/internal/orders"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer stop()

	svc := orders.NewService(loadSKUs("/data/catalog/skus.txt"))
	mux := http.NewServeMux()
	mux.HandleFunc("POST /reservations", func(w http.ResponseWriter, r *http.Request) {
		var res orders.Reservation
		if err := json.NewDecoder(r.Body).Decode(&res); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		svc.Submit(res)
		w.WriteHeader(http.StatusAccepted)
	})
	srv := &http.Server{Addr: ":8080", Handler: mux}

	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error { return svc.Run(ctx) })
	g.Go(srv.ListenAndServe)
	g.Go(func() error {
		<-ctx.Done()
		return srv.Shutdown(context.Background())
	})
	slog.Info("orders-api listening", "addr", srv.Addr)
	if err := g.Wait(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		slog.Error("orders-api stopped", "err", err)
		os.Exit(1)
	}
}

// loadSKUs reads the catalog snapshot that the image ships: one SKU per line.
func loadSKUs(path string) []string {
	f, err := os.Open(path)
	if err != nil {
		slog.Error("cannot read the catalog", "path", path, "err", err)
		os.Exit(1)
	}
	defer f.Close()
	var skus []string
	for scanner := bufio.NewScanner(f); scanner.Scan(); {
		skus = append(skus, scanner.Text())
	}
	return skus
}
