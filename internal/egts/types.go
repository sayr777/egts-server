package egts

import "time"

// Packet types
const (
	PTResponse = 0x00
	PTAppdata  = 0x01
)

// Service types
const (
	SvcAuth     = 0x01
	SvcTeledata = 0x02
)

// Subrecord types
const (
	SRRecordResponse = 0
	SRTermIdentity   = 1
	SRResultCode     = 9
	SRPosData        = 16
	SRExtPosData     = 17
	SRAdSensors      = 18
	SRCounters       = 19
	SRStateData      = 21
	SRAbsAnSens      = 24
	SRAbsCntr        = 25
	SRLiquidLevel    = 27
	SRPassengers     = 28
	SRRadiotagEvent  = 200
	SRIbeaconEvent   = 201
	SRCellInfo       = 202
	SRWifiApData     = 203
)

// EGTS epoch: 2010-01-01 00:00:00 UTC
var EGTSEpoch = time.Date(2010, 1, 1, 0, 0, 0, 0, time.UTC)

// Header is the parsed EGTS packet header.
type Header struct {
	PRV   uint8
	SKID  uint8
	Flags uint8
	HL    uint8  // header length (10 or 11 or more with routing)
	HE    uint8
	FDL   uint16 // frame data length
	PID   uint16 // packet identifier
	PT    uint8  // packet type
	HCS   uint8
}

// Packet is a fully parsed EGTS packet.
type Packet struct {
	Header Header
	PID    uint16
	PT     uint8

	// PT_RESPONSE fields
	ResponsePID uint16
	ProcResult  uint8

	// PT_APPDATA: one or more service data records
	Records []Record
}

// Record is one ServiceDataRecord inside PT_APPDATA.
type Record struct {
	RN    uint16 // record number
	OID   uint32 // object (device) ID, 0 if not present
	HasOID bool
	SST   uint8  // source service type
	RST   uint8  // recipient service type

	Subrecords []Subrecord
}

// Subrecord is one typed payload inside a Record.
type Subrecord struct {
	Type uint8
	Data []byte

	// Decoded fields (populated by DecodeSubrecord)
	PosData     *PosData
	IbeaconData *IbeaconEvent
	CellData    *CellInfo
	WifiData    *WifiApData
	RadioTag    *RadiotagEvent
}

// PosData is SR_POS_DATA (SRT=16).
type PosData struct {
	Time      time.Time
	Lat       float64
	Lon       float64
	Speed     float64 // km/h
	Direction uint16  // degrees 0-359
	Altitude  int32   // metres (0 if not present)
	Odometer  uint32  // km
	Valid     bool
	Fix3D     bool
	Moving    bool
}

// IbeaconEvent is SR_IBEACON_EVENT (SRT=201).
type IbeaconEvent struct {
	EventType uint8  // 1=enter, 2=exit, 3=periodic
	Major     uint16
	Minor     uint16
	RSSI      int8  // dBm
	TxPower   int8  // dBm
	UUID      [16]byte
}

// CellInfo is SR_CELL_INFO (SRT=202).
type CellInfo struct {
	MCC    uint16
	MNC    uint8
	LAC    uint16
	CellID uint32
	RSSI   int8
	RAT    uint8 // 1=GSM, 2=UMTS, 3=LTE, 4=NR
}

// WifiApData is SR_WIFI_AP_DATA (SRT=203).
type WifiApData struct {
	BSSID   [6]byte
	RSSI    int8
	Channel uint8
	SSID    string
}

// RadiotagEvent is SR_RADIOTAG_EVENT (SRT=200).
type RadiotagEvent struct {
	EventType uint8
	TagType   uint8
	UID       []byte
	RSSI      int8
}

// KafkaMessage is the JSON payload written to Kafka.
type KafkaMessage struct {
	PacketID  uint16    `json:"packet_id"`
	DeviceID  uint32    `json:"device_id"`
	ReceivedAt time.Time `json:"received_at"`
	ServiceType uint8   `json:"service_type"`

	// position (present when SR_POS_DATA exists)
	Pos *PosData `json:"position,omitempty"`

	// extension subrecords
	Ibeacons  []IbeaconEvent `json:"ibeacons,omitempty"`
	Cells     []CellInfo     `json:"cells,omitempty"`
	WifiAPs   []WifiApData   `json:"wifi_aps,omitempty"`
	RadioTags []RadiotagEvent `json:"radiotags,omitempty"`

	// raw hex for debugging / replay
	Raw string `json:"raw,omitempty"`
}
