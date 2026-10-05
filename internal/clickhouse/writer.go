// Package clickhouse provides a batching writer for EGTS telemetry data.
package clickhouse

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/sayr777/egts-server/internal/egts"
)

type posRow struct {
	t         time.Time
	deviceID  uint64
	lat, lon  float64
	speed     float32
	direction uint16
	altitude  int32
	odometer  uint64
	valid     uint8
}

// Writer batches rows and flushes to ClickHouse periodically.
type Writer struct {
	conn      driver.Conn
	posCh     chan posRow
	ibeaconCh chan ibeaconRow
	cellCh    chan cellRow
	wifiCh    chan wifiRow
}

type ibeaconRow struct {
	t         time.Time
	deviceID  uint64
	ev        egts.IbeaconEvent
}

type cellRow struct {
	t        time.Time
	deviceID uint64
	c        egts.CellInfo
}

type wifiRow struct {
	t        time.Time
	deviceID uint64
	w        egts.WifiApData
}

func New(addr, database, user, password string) (*Writer, error) {
	conn, err := ch.Open(&ch.Options{
		Addr: []string{addr},
		Auth: ch.Auth{Database: database, Username: user, Password: password},
		DialTimeout:     10 * time.Second,
		MaxOpenConns:    10,
		MaxIdleConns:    5,
		ConnMaxLifetime: time.Hour,
		Compression: &ch.Compression{Method: ch.CompressionLZ4},
	})
	if err != nil {
		return nil, fmt.Errorf("clickhouse open: %w", err)
	}
	w := &Writer{
		conn:      conn,
		posCh:     make(chan posRow, 4096),
		ibeaconCh: make(chan ibeaconRow, 1024),
		cellCh:    make(chan cellRow, 1024),
		wifiCh:    make(chan wifiRow, 1024),
	}
	return w, nil
}

// Start launches flush goroutines. Cancel ctx to stop.
func (w *Writer) Start(ctx context.Context) {
	go w.flush(ctx, "positions", w.flushPositions)
	go w.flush(ctx, "events_ibeacon", w.flushIbeacons)
	go w.flush(ctx, "events_cell", w.flushCells)
	go w.flush(ctx, "events_wifi", w.flushWifi)
}

// flush drains a channel and calls fn every tick or when batch is full.
func (w *Writer) flush(ctx context.Context, name string, fn func(context.Context) error) {
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			fn(context.Background()) // final flush
			return
		case <-ticker.C:
			if err := fn(ctx); err != nil {
				slog.Error("clickhouse flush error", "table", name, "err", err)
			}
		}
	}
}

// WritePosition queues one position row (non-blocking, drops on overflow).
func (w *Writer) WritePosition(t time.Time, deviceID uint64, p *egts.PosData) {
	valid := uint8(0)
	if p.Valid {
		valid = 1
	}
	select {
	case w.posCh <- posRow{t, deviceID, p.Lat, p.Lon, float32(p.Speed),
		p.Direction, p.Altitude, uint64(p.Odometer), valid}:
	default:
		slog.Warn("clickhouse positions channel full, dropping row")
	}
}

// WriteIbeacon queues one iBeacon event.
func (w *Writer) WriteIbeacon(t time.Time, deviceID uint64, ev egts.IbeaconEvent) {
	select {
	case w.ibeaconCh <- ibeaconRow{t, deviceID, ev}:
	default:
	}
}

// WriteCell queues one cell info record.
func (w *Writer) WriteCell(t time.Time, deviceID uint64, c egts.CellInfo) {
	select {
	case w.cellCh <- cellRow{t, deviceID, c}:
	default:
	}
}

// WriteWifi queues one WiFi AP record.
func (w *Writer) WriteWifi(t time.Time, deviceID uint64, ap egts.WifiApData) {
	select {
	case w.wifiCh <- wifiRow{t, deviceID, ap}:
	default:
	}
}

func (w *Writer) flushPositions(ctx context.Context) error {
	n := len(w.posCh)
	if n == 0 {
		return nil
	}
	batch, err := w.conn.PrepareBatch(ctx, "INSERT INTO positions")
	if err != nil {
		return err
	}
	for i := 0; i < n; i++ {
		r := <-w.posCh
		if err := batch.Append(r.t, r.deviceID, r.lat, r.lon, r.speed,
			r.direction, r.altitude, r.odometer, r.valid); err != nil {
			return err
		}
	}
	return batch.Send()
}

func (w *Writer) flushIbeacons(ctx context.Context) error {
	n := len(w.ibeaconCh)
	if n == 0 {
		return nil
	}
	batch, err := w.conn.PrepareBatch(ctx, "INSERT INTO events_ibeacon")
	if err != nil {
		return err
	}
	for i := 0; i < n; i++ {
		r := <-w.ibeaconCh
		uuidStr := fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
			r.ev.UUID[0:4], r.ev.UUID[4:6], r.ev.UUID[6:8],
			r.ev.UUID[8:10], r.ev.UUID[10:16])
		if err := batch.Append(r.t, r.deviceID, r.ev.EventType,
			r.ev.Major, r.ev.Minor, r.ev.RSSI, r.ev.TxPower, uuidStr); err != nil {
			return err
		}
	}
	return batch.Send()
}

func (w *Writer) flushCells(ctx context.Context) error {
	n := len(w.cellCh)
	if n == 0 {
		return nil
	}
	batch, err := w.conn.PrepareBatch(ctx, "INSERT INTO events_cell")
	if err != nil {
		return err
	}
	for i := 0; i < n; i++ {
		r := <-w.cellCh
		if err := batch.Append(r.t, r.deviceID, r.c.MCC, r.c.MNC,
			uint32(r.c.LAC), uint64(r.c.CellID), r.c.RSSI, r.c.RAT); err != nil {
			return err
		}
	}
	return batch.Send()
}

func (w *Writer) flushWifi(ctx context.Context) error {
	n := len(w.wifiCh)
	if n == 0 {
		return nil
	}
	batch, err := w.conn.PrepareBatch(ctx, "INSERT INTO events_wifi")
	if err != nil {
		return err
	}
	for i := 0; i < n; i++ {
		r := <-w.wifiCh
		mac := fmt.Sprintf("%02X:%02X:%02X:%02X:%02X:%02X",
			r.w.BSSID[0], r.w.BSSID[1], r.w.BSSID[2],
			r.w.BSSID[3], r.w.BSSID[4], r.w.BSSID[5])
		if err := batch.Append(r.t, r.deviceID, mac, r.w.SSID, r.w.RSSI, r.w.Channel); err != nil {
			return err
		}
	}
	return batch.Send()
}
