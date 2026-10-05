// Package rediscache manages last-known device positions in Redis
// and publishes live updates for the WebSocket server.
package rediscache

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	deviceTTL    = 10 * time.Minute
	deviceSetKey = "egts:devices"
	pubSubChan   = "egts:positions"
)

// DeviceState is the live state stored per device in Redis.
type DeviceState struct {
	DeviceID  uint64    `json:"device_id"`
	Lat       float64   `json:"lat"`
	Lon       float64   `json:"lon"`
	Speed     float32   `json:"speed"`
	Direction uint16    `json:"direction"`
	UpdatedAt time.Time `json:"updated_at"`
}

// Cache wraps a Redis client.
type Cache struct {
	rdb *redis.Client
}

func New(addr string) *Cache {
	return &Cache{
		rdb: redis.NewClient(&redis.Options{
			Addr:         addr,
			DialTimeout:  5 * time.Second,
			ReadTimeout:  3 * time.Second,
			WriteTimeout: 3 * time.Second,
			PoolSize:     20,
		}),
	}
}

func (c *Cache) Ping(ctx context.Context) error {
	return c.rdb.Ping(ctx).Err()
}

// UpdateDevice stores the device state in a Redis hash and publishes
// the update to the live positions channel.
func (c *Cache) UpdateDevice(ctx context.Context, s DeviceState) {
	key := fmt.Sprintf("egts:device:%d", s.DeviceID)

	pipe := c.rdb.Pipeline()
	pipe.HSet(ctx, key,
		"device_id", s.DeviceID,
		"lat", s.Lat,
		"lon", s.Lon,
		"speed", s.Speed,
		"direction", s.Direction,
		"updated_at", s.UpdatedAt.Unix(),
	)
	pipe.Expire(ctx, key, deviceTTL)
	pipe.SAdd(ctx, deviceSetKey, s.DeviceID)
	pipe.Expire(ctx, deviceSetKey, deviceTTL)

	if _, err := pipe.Exec(ctx); err != nil {
		slog.Warn("redis update device failed", "device_id", s.DeviceID, "err", err)
		return
	}

	// Publish for WebSocket fan-out
	payload, _ := json.Marshal(s)
	if err := c.rdb.Publish(ctx, pubSubChan, payload).Err(); err != nil {
		slog.Warn("redis publish failed", "err", err)
	}
}

// AllDevices returns all active device states (used for initial WebSocket load).
func (c *Cache) AllDevices(ctx context.Context) ([]DeviceState, error) {
	ids, err := c.rdb.SMembers(ctx, deviceSetKey).Result()
	if err != nil {
		return nil, err
	}

	pipe := c.rdb.Pipeline()
	cmds := make([]*redis.MapStringStringCmd, len(ids))
	for i, id := range ids {
		cmds[i] = pipe.HGetAll(ctx, "egts:device:"+id)
	}
	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		return nil, err
	}

	var devices []DeviceState
	for _, cmd := range cmds {
		m, err := cmd.Result()
		if err != nil || len(m) == 0 {
			continue
		}
		var s DeviceState
		if err := unmarshalDevice(m, &s); err == nil {
			devices = append(devices, s)
		}
	}
	return devices, nil
}

// Subscribe returns a pubsub subscription to the live positions channel.
func (c *Cache) Subscribe(ctx context.Context) *redis.PubSub {
	return c.rdb.Subscribe(ctx, pubSubChan)
}

func unmarshalDevice(m map[string]string, s *DeviceState) error {
	var err error
	_, err = fmt.Sscan(m["device_id"], &s.DeviceID)
	if err != nil {
		return err
	}
	fmt.Sscan(m["lat"], &s.Lat)
	fmt.Sscan(m["lon"], &s.Lon)
	fmt.Sscan(m["speed"], &s.Speed)
	fmt.Sscan(m["direction"], &s.Direction)
	var ts int64
	fmt.Sscan(m["updated_at"], &ts)
	s.UpdatedAt = time.Unix(ts, 0).UTC()
	return nil
}
