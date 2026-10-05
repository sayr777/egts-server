package main

import (
	"context"
	"encoding/hex"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/sayr777/egts-server/config"
	"github.com/sayr777/egts-server/internal/egts"
	kafkapkg "github.com/sayr777/egts-server/internal/kafka"
	"github.com/sayr777/egts-server/internal/metrics"
)

func main() {
	cfgPath := flag.String("config", "", "path to config.yaml (optional, env vars take priority)")
	flag.Parse()

	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})))

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		slog.Error("config load failed", "err", err)
		os.Exit(1)
	}

	metrics.Register()

	producer := kafkapkg.NewProducer(
		cfg.Kafka.Brokers,
		[]string{cfg.Kafka.TopicRaw, cfg.Kafka.TopicPos, cfg.Kafka.TopicEvents},
		cfg.Kafka.BatchSize,
		cfg.Kafka.BatchTimeout,
	)
	defer producer.Close()

	// Prometheus metrics endpoint
	go func() {
		mux := http.NewServeMux()
		mux.Handle("/metrics", metrics.Handler())
		mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
		})
		slog.Info("metrics listening", "addr", cfg.Server.MetricsAddr)
		if err := http.ListenAndServe(cfg.Server.MetricsAddr, mux); err != nil {
			slog.Error("metrics server failed", "err", err)
		}
	}()

	ln, err := net.Listen("tcp", cfg.Server.Addr)
	if err != nil {
		slog.Error("listen failed", "addr", cfg.Server.Addr, "err", err)
		os.Exit(1)
	}
	slog.Info("EGTS server listening", "addr", cfg.Server.Addr)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Semaphore to cap concurrent connections
	sem := make(chan struct{}, cfg.Server.MaxConnections)
	var wg sync.WaitGroup

	go func() {
		<-ctx.Done()
		slog.Info("shutdown signal received")
		ln.Close()
	}()

	var pidCounter atomic.Uint32

	for {
		conn, err := ln.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				break
			default:
				slog.Error("accept error", "err", err)
				continue
			}
			break
		}

		select {
		case sem <- struct{}{}:
		default:
			slog.Warn("connection limit reached, dropping connection",
				"remote", conn.RemoteAddr())
			conn.Close()
			continue
		}

		wg.Add(1)
		go func(c net.Conn) {
			defer func() {
				c.Close()
				<-sem
				wg.Done()
				metrics.ActiveConnections.Dec()
			}()
			metrics.ActiveConnections.Inc()
			handleDevice(c, producer, cfg, &pidCounter)
		}(conn)
	}

	wg.Wait()
	slog.Info("all connections closed, exiting")
}

// handleDevice is one goroutine per TCP connection.
// It reads packets in a loop, ACKs immediately, then pushes to Kafka.
func handleDevice(conn net.Conn, producer *kafkapkg.Producer, cfg *config.Config, pidCounter *atomic.Uint32) {
	remote := conn.RemoteAddr().String()
	slog.Info("device connected", "remote", remote)
	defer slog.Info("device disconnected", "remote", remote)

	ctx := context.Background()
	var deviceID uint32 // learned from first packet with OID

	for {
		start := time.Now()

		raw, err := egts.ReadPacket(conn, cfg.Server.ReadTimeout)
		if err != nil {
			logConnError(remote, err)
			return
		}

		pkt, err := egts.Parse(raw)
		if err != nil {
			metrics.ParseErrors.WithLabelValues(err.Error()).Inc()
			slog.Warn("parse error", "remote", remote, "err", err)
			return
		}

		// ACK the device immediately, before Kafka write
		ourPID := uint16(pidCounter.Add(1) & 0xFFFF)
		ack := egts.BuildPTResponse(pkt.PID, egts.ProcResultOK, ourPID)
		conn.SetWriteDeadline(time.Now().Add(cfg.Server.WriteTimeout))
		if _, err := conn.Write(ack); err != nil {
			slog.Warn("write ack failed", "remote", remote, "err", err)
			return
		}

		metrics.PacketsTotal.WithLabelValues(packetTypeName(pkt.PT)).Inc()

		// Extract device ID from first record with OID
		for _, rec := range pkt.Records {
			if rec.HasOID && rec.OID != 0 {
				deviceID = rec.OID
				break
			}
		}

		// Build Kafka message and publish (async, non-blocking)
		if pkt.PT == egts.PTAppdata && len(pkt.Records) > 0 {
			msg := buildKafkaMessage(pkt, deviceID, raw)
			key := []byte(strconv.FormatUint(uint64(deviceID), 10))

			// Always send to raw topic
			if err := producer.Produce(ctx, cfg.Kafka.TopicRaw, key, msg); err != nil {
				metrics.KafkaErrorsTotal.WithLabelValues(cfg.Kafka.TopicRaw).Inc()
				slog.Warn("kafka produce raw failed", "device_id", deviceID, "err", err)
			}

			// Send position to dedicated topic when available
			if msg.Pos != nil {
				posMsg := struct {
					PacketID  uint16         `json:"packet_id"`
					DeviceID  uint32         `json:"device_id"`
					ReceivedAt time.Time     `json:"received_at"`
					Pos       *egts.PosData  `json:"position"`
				}{msg.PacketID, msg.DeviceID, msg.ReceivedAt, msg.Pos}
				if err := producer.Produce(ctx, cfg.Kafka.TopicPos, key, posMsg); err != nil {
					metrics.KafkaErrorsTotal.WithLabelValues(cfg.Kafka.TopicPos).Inc()
				}
			}

			// Send extension events (iBeacon, LBS, WiFi, RFID) if present
			hasEvents := len(msg.Ibeacons) > 0 || len(msg.Cells) > 0 ||
				len(msg.WifiAPs) > 0 || len(msg.RadioTags) > 0
			if hasEvents {
				if err := producer.Produce(ctx, cfg.Kafka.TopicEvents, key, msg); err != nil {
					metrics.KafkaErrorsTotal.WithLabelValues(cfg.Kafka.TopicEvents).Inc()
				}
			}
		}

		metrics.PacketProcessDuration.Observe(time.Since(start).Seconds())
	}
}

func buildKafkaMessage(pkt *egts.Packet, deviceID uint32, raw []byte) *egts.KafkaMessage {
	msg := &egts.KafkaMessage{
		PacketID:   pkt.PID,
		DeviceID:   deviceID,
		ReceivedAt: time.Now().UTC(),
		Raw:        hex.EncodeToString(raw),
	}
	for _, rec := range pkt.Records {
		msg.ServiceType = rec.SST
		for _, sr := range rec.Subrecords {
			switch {
			case sr.PosData != nil:
				msg.Pos = sr.PosData
			case sr.IbeaconData != nil:
				msg.Ibeacons = append(msg.Ibeacons, *sr.IbeaconData)
			case sr.CellData != nil:
				msg.Cells = append(msg.Cells, *sr.CellData)
			case sr.WifiData != nil:
				msg.WifiAPs = append(msg.WifiAPs, *sr.WifiData)
			case sr.RadioTag != nil:
				msg.RadioTags = append(msg.RadioTags, *sr.RadioTag)
			}
		}
	}
	return msg
}

func packetTypeName(pt uint8) string {
	switch pt {
	case egts.PTResponse:
		return "PT_RESPONSE"
	case egts.PTAppdata:
		return "PT_APPDATA"
	default:
		return fmt.Sprintf("PT_%d", pt)
	}
}

func logConnError(remote string, err error) {
	if isConnClosed(err) {
		slog.Info("device disconnected cleanly", "remote", remote)
		return
	}
	slog.Warn("connection error", "remote", remote, "err", err)
}

func isConnClosed(err error) bool {
	if err == nil {
		return false
	}
	s := err.Error()
	return s == "EOF" ||
		contains(s, "connection reset") ||
		contains(s, "broken pipe") ||
		contains(s, "use of closed")
}

func contains(s, sub string) bool {
	return len(s) >= len(sub) && (s == sub || len(s) > 0 && containsStr(s, sub))
}

func containsStr(s, sub string) bool {
	for i := 0; i <= len(s)-len(sub); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
