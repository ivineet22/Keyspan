package api

import (
	"context"
	"errors"
	"io"
	"net/http"

	"store/internal/router"
)

const maxValueBytes = 1 << 20

type Service interface {
	Put(ctx context.Context, tenant, key string, value []byte) error
	Get(ctx context.Context, tenant, key string) ([]byte, error)
}

func New(svc Service) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("PUT /v1/tenants/{tenant}/keys/{key}", putKey(svc))
	mux.HandleFunc("GET /v1/tenants/{tenant}/keys/{key}", getKey(svc))
	return mux
}

func putKey(svc Service) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		value, err := io.ReadAll(io.LimitReader(r.Body, maxValueBytes+1))
		if err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		if len(value) > maxValueBytes {
			http.Error(w, "value too large", http.StatusRequestEntityTooLarge)
			return
		}
		if err := svc.Put(r.Context(), r.PathValue("tenant"), r.PathValue("key"), value); err != nil {
			writeErr(w, err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}
}

func getKey(svc Service) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		value, err := svc.Get(r.Context(), r.PathValue("tenant"), r.PathValue("key"))
		if err != nil {
			writeErr(w, err)
			return
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Write(value)
	}
}

func writeErr(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, router.ErrNotFound):
		http.Error(w, "not found", http.StatusNotFound)
	case errors.Is(err, router.ErrWrongShard):
		http.Error(w, "wrong shard", http.StatusConflict)
	case errors.Is(err, router.ErrStaleEpoch):
		http.Error(w, "stale epoch", http.StatusConflict)
	case errors.Is(err, router.ErrRetry):
		http.Error(w, "retry", http.StatusServiceUnavailable)
	case errors.Is(err, router.ErrUnavailable):
		http.Error(w, "range unavailable", http.StatusServiceUnavailable)
	default:
		http.Error(w, "internal error", http.StatusInternalServerError)
	}
}
