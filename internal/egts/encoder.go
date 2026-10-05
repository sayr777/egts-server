package egts

import "encoding/binary"

// BuildPTResponse builds a minimal PT_RESPONSE packet.
// Called immediately after parsing an incoming packet to ACK the device.
func BuildPTResponse(responsePID uint16, procResult uint8, ourPID uint16) []byte {
	// SFRD = RPID(2) + PR(1) = 3 bytes
	sfrd := make([]byte, 3)
	binary.LittleEndian.PutUint16(sfrd[0:2], responsePID)
	sfrd[2] = procResult

	// Header: PRV SKID FLAGS HL HE FDL(2) PID(2) PT HCS
	hdr := make([]byte, 11)
	hdr[0] = 0x01 // PRV
	hdr[1] = 0x00 // SKID
	hdr[2] = 0x00 // FLAGS
	hdr[3] = 0x0B // HL = 11
	hdr[4] = 0x00 // HE
	binary.LittleEndian.PutUint16(hdr[5:7], uint16(len(sfrd)))
	binary.LittleEndian.PutUint16(hdr[7:9], ourPID)
	hdr[9] = PTResponse
	hdr[10] = CRC8(hdr[:10])

	sfrcs := CRC16(sfrd)
	tail := make([]byte, 2)
	binary.LittleEndian.PutUint16(tail, sfrcs)

	pkt := make([]byte, 0, len(hdr)+len(sfrd)+2)
	pkt = append(pkt, hdr...)
	pkt = append(pkt, sfrd...)
	pkt = append(pkt, tail...)
	return pkt
}

// ProcResultOK is the standard "no error" processing result code.
const ProcResultOK = 0x00
