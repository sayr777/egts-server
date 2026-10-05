package egts

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"time"
)

var (
	ErrTooShort      = errors.New("packet too short")
	ErrBadVersion    = errors.New("unsupported EGTS version")
	ErrBadHeaderCRC  = errors.New("header CRC mismatch")
	ErrBadBodyCRC    = errors.New("body CRC mismatch")
)

// minHeaderSize is the minimum packet header without routing fields.
const minHeaderSize = 10

// ReadPacket reads one complete EGTS packet from conn.
// It performs two reads: header first, then body — zero copy on the heap.
func ReadPacket(conn net.Conn, deadline time.Duration) ([]byte, error) {
	if deadline > 0 {
		conn.SetReadDeadline(time.Now().Add(deadline))
	}

	// 1. Read fixed-size header prefix (first 10 bytes cover up to HCS for HL=10,
	//    but HL could be larger if routing fields are present).
	prefix := make([]byte, minHeaderSize)
	if _, err := io.ReadFull(conn, prefix); err != nil {
		return nil, err
	}

	if prefix[0] != 0x01 {
		return nil, ErrBadVersion
	}
	hl := int(prefix[3])
	if hl < minHeaderSize {
		return nil, ErrTooShort
	}

	// 2. Read remaining header bytes if HL > 10.
	var header []byte
	if hl > minHeaderSize {
		rest := make([]byte, hl-minHeaderSize)
		if _, err := io.ReadFull(conn, rest); err != nil {
			return nil, err
		}
		header = append(prefix, rest...)
	} else {
		header = prefix
	}

	// 3. Verify header CRC (last byte of header).
	if CRC8(header[:hl-1]) != header[hl-1] {
		return nil, ErrBadHeaderCRC
	}

	// 4. Read body (SFRD + 2-byte SFRCS).
	fdl := int(binary.LittleEndian.Uint16(header[5:7]))
	var body []byte
	if fdl > 0 {
		body = make([]byte, fdl+2)
		if _, err := io.ReadFull(conn, body); err != nil {
			return nil, err
		}
		sfrd := body[:fdl]
		sfrcs := binary.LittleEndian.Uint16(body[fdl:])
		if CRC16(sfrd) != sfrcs {
			return nil, ErrBadBodyCRC
		}
	}

	return append(header, body...), nil
}

// Parse decodes a raw EGTS packet byte slice.
func Parse(raw []byte) (*Packet, error) {
	if len(raw) < minHeaderSize {
		return nil, ErrTooShort
	}

	hl := int(raw[3])
	fdl := int(binary.LittleEndian.Uint16(raw[5:7]))
	pid := binary.LittleEndian.Uint16(raw[7:9])
	pt := raw[9]

	pkt := &Packet{PID: pid, PT: pt}

	if fdl == 0 {
		return pkt, nil
	}

	sfrd := raw[hl : hl+fdl]

	switch pt {
	case PTResponse:
		if len(sfrd) < 3 {
			return pkt, nil
		}
		pkt.ResponsePID = binary.LittleEndian.Uint16(sfrd[0:2])
		pkt.ProcResult = sfrd[2]

	case PTAppdata:
		records, err := parseServiceDataSet(sfrd)
		if err != nil {
			return pkt, fmt.Errorf("parse service data set: %w", err)
		}
		pkt.Records = records
	}

	return pkt, nil
}

func parseServiceDataSet(data []byte) ([]Record, error) {
	var records []Record
	pos := 0
	for pos+7 <= len(data) {
		rl := int(binary.LittleEndian.Uint16(data[pos : pos+2]))
		rn := binary.LittleEndian.Uint16(data[pos+2 : pos+4])
		flags := data[pos+4]
		pos += 5

		rec := Record{RN: rn}

		// OBF bit (bit 0 of flags) → OID field present
		if flags&0x01 != 0 {
			if pos+4 > len(data) {
				break
			}
			rec.OID = binary.LittleEndian.Uint32(data[pos : pos+4])
			rec.HasOID = true
			pos += 4
		}
		// EVFE bit (bit 1) → EVID field present
		if flags&0x02 != 0 {
			pos += 4
		}
		// TMFE bit (bit 2) → TM field present
		if flags&0x04 != 0 {
			pos += 4
		}

		if pos+2 > len(data) {
			break
		}
		rec.SST = data[pos]
		rec.RST = data[pos+1]
		pos += 2

		rdEnd := pos + rl
		if rdEnd > len(data) {
			rdEnd = len(data)
		}
		rec.Subrecords = parseRecordDataSet(data[pos:rdEnd])
		pos = rdEnd

		records = append(records, rec)
	}
	return records, nil
}

func parseRecordDataSet(data []byte) []Subrecord {
	var srs []Subrecord
	pos := 0
	for pos+3 <= len(data) {
		srt := data[pos]
		srl := int(binary.LittleEndian.Uint16(data[pos+1 : pos+3]))
		pos += 3
		if pos+srl > len(data) {
			break
		}
		srData := data[pos : pos+srl]
		sr := Subrecord{Type: srt, Data: srData}
		decodeSubrecord(&sr)
		srs = append(srs, sr)
		pos += srl
	}
	return srs
}

func decodeSubrecord(sr *Subrecord) {
	switch sr.Type {
	case SRPosData:
		sr.PosData = decodePosData(sr.Data)
	case SRIbeaconEvent:
		sr.IbeaconData = decodeIbeaconEvent(sr.Data)
	case SRCellInfo:
		sr.CellData = decodeCellInfo(sr.Data)
	case SRWifiApData:
		sr.WifiData = decodeWifiApData(sr.Data)
	case SRRadiotagEvent:
		sr.RadioTag = decodeRadiotagEvent(sr.Data)
	}
}

func decodePosData(d []byte) *PosData {
	if len(d) < 21 {
		return nil
	}
	ntmSec := binary.LittleEndian.Uint32(d[0:4])
	t := EGTSEpoch.Add(time.Duration(ntmSec) * time.Second)

	latRaw := binary.LittleEndian.Uint32(d[4:8])
	lonRaw := binary.LittleEndian.Uint32(d[8:12])
	lat := float64(latRaw) * 90.0 / math.MaxUint32
	lon := float64(lonRaw) * 180.0 / math.MaxUint32

	flags := d[12]
	// bit7=ALTE, bit6=LOHS, bit5=LAHS, bit4=MV, bit3=BB, bit2=CS, bit1=FIX, bit0=VLD
	if flags&0x20 != 0 { // LAHS=1 → South
		lat = -lat
	}
	if flags&0x40 != 0 { // LOHS=1 → West
		lon = -lon
	}

	spdRaw := binary.LittleEndian.Uint16(d[13:15])
	dirH := (spdRaw >> 15) & 1
	speed := float64(spdRaw&0x3FFF) / 10.0
	dir := uint16(d[15]) | (dirH << 7)

	odmBytes := [4]byte{}
	copy(odmBytes[:3], d[16:19])
	odm := binary.LittleEndian.Uint32(odmBytes[:])

	pos := &PosData{
		Time:      t,
		Lat:       lat,
		Lon:       lon,
		Speed:     speed,
		Direction: dir,
		Odometer:  odm,
		Valid:     flags&0x01 != 0,
		Fix3D:     flags&0x02 != 0,
		Moving:    flags&0x10 != 0,
	}

	// Optional altitude (ALTE flag = bit7)
	if flags&0x80 != 0 && len(d) >= 24 {
		altBytes := [4]byte{}
		copy(altBytes[:3], d[21:24])
		altSign := int32(1)
		// ALTS bit is bit14 of SPD field
		if (spdRaw>>14)&1 != 0 {
			altSign = -1
		}
		pos.Altitude = altSign * int32(binary.LittleEndian.Uint32(altBytes[:]))
	}

	return pos
}

func decodeIbeaconEvent(d []byte) *IbeaconEvent {
	if len(d) < 23 {
		return nil
	}
	ev := &IbeaconEvent{
		EventType: d[0],
		Major:     binary.LittleEndian.Uint16(d[1:3]),
		Minor:     binary.LittleEndian.Uint16(d[3:5]),
		RSSI:    int8(d[5] - 128),
		TxPower: int8(d[6] - 128),
	}
	copy(ev.UUID[:], d[7:23])
	return ev
}

func decodeCellInfo(d []byte) *CellInfo {
	if len(d) < 11 {
		return nil
	}
	return &CellInfo{
		MCC:    binary.LittleEndian.Uint16(d[0:2]),
		MNC:    d[2],
		LAC:    binary.LittleEndian.Uint16(d[3:5]),
		CellID: binary.LittleEndian.Uint32(d[5:9]),
		RSSI:   int8(d[9] - 128),
		RAT:    d[10],
	}
}

func decodeWifiApData(d []byte) *WifiApData {
	if len(d) < 9 {
		return nil
	}
	ap := &WifiApData{
		RSSI:    int8(d[6] - 128),
		Channel: d[7],
	}
	copy(ap.BSSID[:], d[0:6])
	ssidLen := int(d[8])
	if ssidLen > 0 && len(d) >= 9+ssidLen {
		ap.SSID = string(d[9 : 9+ssidLen])
	}
	return ap
}

func decodeRadiotagEvent(d []byte) *RadiotagEvent {
	if len(d) < 4 {
		return nil
	}
	uidLen := int(d[2])
	if len(d) < 3+uidLen+1 {
		return nil
	}
	ev := &RadiotagEvent{
		EventType: d[0],
		TagType:   d[1],
		UID:       make([]byte, uidLen),
		RSSI:      int8(d[3+uidLen] - 128),
	}
	copy(ev.UID, d[3:3+uidLen])
	return ev
}
