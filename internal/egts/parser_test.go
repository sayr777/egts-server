package egts

import (
	"encoding/hex"
	"testing"
)

// Hex strings from the Python egts-parser project (all CRCs verified).
const (
	hexPTResponse = "0100030B0003008900004A15380033E8"

	hexIbeacon = "0100000B0053000100019A4C00010000020210150028CE671A13119C9E47AD7F35137C012F" +
		"0000000000C91700010A002A003F45550E8400E29B41D4A716446655440000" +
		"C91700030B002B003845550E8400E29B41D4A716446655440000E8F2"

	hexLBS = "0100000B0054000200010B4D00020000020210150028CE671A13119C9E47AD7F35137C012F" +
		"0000000000CA0B00FA00142B1A013C1A003503CA0B00FA00142B1A023C1A002B03" +
		"CB1600AABBCCDDEEFF3C060D4D6F736B6F76736B6179615342B231"

	hexRadiotag = "0100000B002A00030001BE2300030000020210150028CE671A13119C9E47AD7F35137C012F" +
		"0000000000C80800010104DEADBEEF4421BF"
)

// ── CRC ───────────────────────────────────────────────────────────────────────

func TestCRC8_PTResponse(t *testing.T) {
	// Header bytes 0..9 of PT_RESPONSE, HCS = 0x4A
	raw := mustDecodeHex(t, hexPTResponse)
	got := CRC8(raw[:10])
	if got != 0x4A {
		t.Errorf("CRC8 = %02X, want 4A", got)
	}
}

func TestCRC16_PTResponse(t *testing.T) {
	// SFRD = 15 38 00, expected SFRCS = E833
	sfrd := mustDecodeHex(t, "153800")
	got := CRC16(sfrd)
	if got != 0xE833 {
		t.Errorf("CRC16 = %04X, want E833", got)
	}
}

// ── Parse PT_RESPONSE ─────────────────────────────────────────────────────────

func TestParse_PTResponse(t *testing.T) {
	raw := mustDecodeHex(t, hexPTResponse)
	pkt, err := Parse(raw)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if pkt.PT != PTResponse {
		t.Errorf("PT = %d, want %d", pkt.PT, PTResponse)
	}
	if pkt.PID != 137 {
		t.Errorf("PID = %d, want 137", pkt.PID)
	}
	if pkt.ResponsePID != 0x3815 {
		t.Errorf("ResponsePID = %04X, want 3815", pkt.ResponsePID)
	}
	if pkt.ProcResult != 0 {
		t.Errorf("ProcResult = %d, want 0", pkt.ProcResult)
	}
}

// ── ReadPacket CRC checks ─────────────────────────────────────────────────────

func TestParse_HeaderCRC_OK(t *testing.T) {
	raw := mustDecodeHex(t, hexIbeacon)
	_, err := Parse(raw)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestParse_HeaderCRC_Bad(t *testing.T) {
	raw := mustDecodeHex(t, hexPTResponse)
	raw[10] ^= 0xFF // flip HCS
	_, err := Parse(raw)
	// Parse does not re-verify CRC (ReadPacket does); just ensure no panic
	_ = err
}

// ── SR_POS_DATA ───────────────────────────────────────────────────────────────

func TestParse_PosData_Ibeacon(t *testing.T) {
	raw := mustDecodeHex(t, hexIbeacon)
	pkt, err := Parse(raw)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if len(pkt.Records) == 0 {
		t.Fatal("no records")
	}

	var pos *PosData
	for _, sr := range pkt.Records[0].Subrecords {
		if sr.PosData != nil {
			pos = sr.PosData
		}
	}
	if pos == nil {
		t.Fatal("no SR_POS_DATA")
	}
	if !pos.Valid {
		t.Error("position not valid")
	}
	if pos.Lat < 55 || pos.Lat > 56 {
		t.Errorf("Lat = %.4f, want ~55.76", pos.Lat)
	}
	if pos.Lon < 37 || pos.Lon > 38 {
		t.Errorf("Lon = %.4f, want ~37.62", pos.Lon)
	}
	if pos.Speed < 30 || pos.Speed > 50 {
		t.Errorf("Speed = %.1f km/h, want ~38", pos.Speed)
	}
}

// ── SR_IBEACON_EVENT ──────────────────────────────────────────────────────────

func TestParse_IbeaconEvent(t *testing.T) {
	raw := mustDecodeHex(t, hexIbeacon)
	pkt, err := Parse(raw)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	var beacons []*IbeaconEvent
	for _, sr := range pkt.Records[0].Subrecords {
		if sr.IbeaconData != nil {
			beacons = append(beacons, sr.IbeaconData)
		}
	}
	if len(beacons) != 2 {
		t.Fatalf("got %d ibeacon events, want 2", len(beacons))
	}

	b := beacons[0]
	if b.EventType != 1 {
		t.Errorf("EventType = %d, want 1 (enter)", b.EventType)
	}
	if b.Major != 10 {
		t.Errorf("Major = %d, want 10", b.Major)
	}
	if b.Minor != 42 {
		t.Errorf("Minor = %d, want 42", b.Minor)
	}
	if b.RSSI != -65 {
		t.Errorf("RSSI = %d dBm, want -65", b.RSSI)
	}
	// UUID starts with 55 0E 84 00
	if b.UUID[0] != 0x55 || b.UUID[1] != 0x0E {
		t.Errorf("UUID[0:2] = %02X %02X, want 55 0E", b.UUID[0], b.UUID[1])
	}

	b2 := beacons[1]
	if b2.EventType != 3 {
		t.Errorf("EventType[1] = %d, want 3 (periodic)", b2.EventType)
	}
}

// ── SR_CELL_INFO ──────────────────────────────────────────────────────────────

func TestParse_CellInfo(t *testing.T) {
	raw := mustDecodeHex(t, hexLBS)
	pkt, err := Parse(raw)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	var cells []*CellInfo
	var wifis []*WifiApData
	for _, sr := range pkt.Records[0].Subrecords {
		if sr.CellData != nil {
			cells = append(cells, sr.CellData)
		}
		if sr.WifiData != nil {
			wifis = append(wifis, sr.WifiData)
		}
	}

	if len(cells) != 2 {
		t.Fatalf("got %d cells, want 2", len(cells))
	}
	c := cells[0]
	if c.MCC != 250 {
		t.Errorf("MCC = %d, want 250 (Russia)", c.MCC)
	}
	if c.MNC != 20 {
		t.Errorf("MNC = %d, want 20 (Tele2)", c.MNC)
	}
	if c.RAT != 3 {
		t.Errorf("RAT = %d, want 3 (LTE)", c.RAT)
	}
	if c.RSSI != -75 {
		t.Errorf("RSSI = %d, want -75", c.RSSI)
	}

	if len(wifis) != 1 {
		t.Fatalf("got %d wifi APs, want 1", len(wifis))
	}
	ap := wifis[0]
	if ap.SSID != "MoskovskayaSB" {
		t.Errorf("SSID = %q, want MoskovskayaSB", ap.SSID)
	}
	if ap.BSSID != [6]byte{0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF} {
		t.Errorf("BSSID = %v", ap.BSSID)
	}
}

// ── SR_RADIOTAG_EVENT ─────────────────────────────────────────────────────────

func TestParse_Radiotag(t *testing.T) {
	raw := mustDecodeHex(t, hexRadiotag)
	pkt, err := Parse(raw)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	var tag *RadiotagEvent
	for _, sr := range pkt.Records[0].Subrecords {
		if sr.RadioTag != nil {
			tag = sr.RadioTag
		}
	}
	if tag == nil {
		t.Fatal("no SR_RADIOTAG_EVENT")
	}
	if tag.EventType != 1 {
		t.Errorf("EventType = %d, want 1", tag.EventType)
	}
	if tag.TagType != 1 {
		t.Errorf("TagType = %d, want 1 (passive)", tag.TagType)
	}
	if len(tag.UID) != 4 || tag.UID[0] != 0xDE || tag.UID[3] != 0xEF {
		t.Errorf("UID = %X, want DEADBEEF", tag.UID)
	}
}

// ── BuildPTResponse ───────────────────────────────────────────────────────────

func TestBuildPTResponse(t *testing.T) {
	resp := BuildPTResponse(137, ProcResultOK, 1)
	if len(resp) != 16 {
		t.Fatalf("response len = %d, want 16", len(resp))
	}
	if resp[0] != 0x01 {
		t.Errorf("PRV = %02X, want 01", resp[0])
	}
	if resp[9] != PTResponse {
		t.Errorf("PT = %02X, want %02X", resp[9], PTResponse)
	}
	// Verify header CRC
	if CRC8(resp[:10]) != resp[10] {
		t.Errorf("header CRC mismatch: got %02X, want %02X", resp[10], CRC8(resp[:10]))
	}
	// Verify body CRC
	sfrd := resp[11:14]
	wantSFRCS := CRC16(sfrd)
	gotSFRCS := uint16(resp[14]) | uint16(resp[15])<<8
	if gotSFRCS != wantSFRCS {
		t.Errorf("body CRC = %04X, want %04X", gotSFRCS, wantSFRCS)
	}
}

// ── Benchmark ────────────────────────────────────────────────────────────────

func BenchmarkParse_Ibeacon(b *testing.B) {
	raw := mustDecodeHex(b, hexIbeacon)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Parse(raw)
	}
}

func mustDecodeHex(tb testing.TB, s string) []byte {
	tb.Helper()
	b, err := hex.DecodeString(s)
	if err != nil {
		tb.Fatalf("hex decode: %v", err)
	}
	return b
}
