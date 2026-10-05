// wsserver serves the WebSocket live feed and REST API for the map frontend.
package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/gorilla/websocket"

	"github.com/sayr777/egts-server/internal/rediscache"
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  256,
	WriteBufferSize: 4096,
	CheckOrigin:    func(_ *http.Request) bool { return true }, // allow all origins
}

// hub manages all active WebSocket connections.
type hub struct {
	mu      sync.RWMutex
	clients map[*websocket.Conn]struct{}
}

func newHub() *hub {
	return &hub{clients: make(map[*websocket.Conn]struct{})}
}

func (h *hub) add(c *websocket.Conn) {
	h.mu.Lock()
	h.clients[c] = struct{}{}
	h.mu.Unlock()
}

func (h *hub) remove(c *websocket.Conn) {
	h.mu.Lock()
	delete(h.clients, c)
	h.mu.Unlock()
}

func (h *hub) broadcast(msg []byte) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	for c := range h.clients {
		c.SetWriteDeadline(time.Now().Add(5 * time.Second))
		if err := c.WriteMessage(websocket.TextMessage, msg); err != nil {
			c.Close()
		}
	}
}

func (h *hub) count() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.clients)
}

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	addr := envOr("WS_ADDR", ":8081")
	redisAddr := envOr("REDIS_ADDR", "redis:6379")

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	cache := rediscache.New(redisAddr)
	for i := 0; i < 30; i++ {
		if err := cache.Ping(ctx); err == nil {
			break
		}
		slog.Info("waiting for redis...", "attempt", i+1)
		time.Sleep(2 * time.Second)
	}
	slog.Info("redis connected", "addr", redisAddr)

	h := newHub()

	// Redis Pub/Sub → broadcast to all WebSocket clients
	go func() {
		ps := cache.Subscribe(ctx)
		defer ps.Close()
		ch := ps.Channel()
		for {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-ch:
				if !ok {
					return
				}
				h.broadcast([]byte(msg.Payload))
			}
		}
	}()

	// Log connection count every minute
	go func() {
		t := time.NewTicker(time.Minute)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				slog.Info("ws connections", "count", h.count())
			}
		}
	}()

	mux := http.NewServeMux()

	// WebSocket endpoint
	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			slog.Warn("ws upgrade failed", "err", err)
			return
		}
		h.add(conn)
		defer func() {
			h.remove(conn)
			conn.Close()
		}()

		// Send all current device positions on connect
		devices, err := cache.AllDevices(r.Context())
		if err == nil && len(devices) > 0 {
			payload, _ := json.Marshal(map[string]any{
				"type":    "snapshot",
				"devices": devices,
			})
			conn.WriteMessage(websocket.TextMessage, payload)
		}

		// Keep-alive: drain incoming pings, detect disconnect
		conn.SetReadDeadline(time.Now().Add(90 * time.Second))
		conn.SetPongHandler(func(_ string) error {
			conn.SetReadDeadline(time.Now().Add(90 * time.Second))
			return nil
		})
		go func() {
			t := time.NewTicker(30 * time.Second)
			defer t.Stop()
			for range t.C {
				if conn.WriteMessage(websocket.PingMessage, nil) != nil {
					return
				}
			}
		}()

		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	})

	// REST: all devices (last known)
	mux.HandleFunc("/api/devices", func(w http.ResponseWriter, r *http.Request) {
		devices, err := cache.AllDevices(r.Context())
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Access-Control-Allow-Origin", "*")
		json.NewEncoder(w).Encode(devices)
	})

	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		if err := cache.Ping(r.Context()); err != nil {
			http.Error(w, "redis unavailable", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	})

	srv := &http.Server{Addr: addr, Handler: mux}

	go func() {
		slog.Info("wsserver listening", "addr", addr)
		if err := srv.ListenAndServe(); err != http.ErrServerClosed {
			slog.Error("wsserver failed", "err", err)
		}
	}()

	<-ctx.Done()
	shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	srv.Shutdown(shutCtx)
	slog.Info("wsserver stopped")
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
