package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	ActiveConnections = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "egts_active_connections",
		Help: "Number of currently open device connections.",
	})

	PacketsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "egts_packets_total",
		Help: "Total EGTS packets received, by type.",
	}, []string{"type"})

	ParseErrors = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "egts_parse_errors_total",
		Help: "Total packet parse errors, by reason.",
	}, []string{"reason"})

	KafkaProducedTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "egts_kafka_produced_total",
		Help: "Total messages produced to Kafka, by topic.",
	}, []string{"topic"})

	KafkaErrorsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "egts_kafka_errors_total",
		Help: "Total Kafka producer errors, by topic.",
	}, []string{"topic"})

	PacketProcessDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "egts_packet_process_seconds",
		Help:    "Time to parse and forward one packet.",
		Buckets: prometheus.DefBuckets,
	})
)

func Register() {
	prometheus.MustRegister(
		ActiveConnections,
		PacketsTotal,
		ParseErrors,
		KafkaProducedTotal,
		KafkaErrorsTotal,
		PacketProcessDuration,
	)
}

func Handler() http.Handler {
	return promhttp.Handler()
}
