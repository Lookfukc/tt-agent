package entry

import (
	"bufio"
	"context"
	"crypto/sha1"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"time"
)

// wsGUID is the RFC6455 handshake magic value.
const wsGUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

// WS opcodes
const (
	wsOpContinuation = 0x0
	wsOpText         = 0x1
	wsOpBinary       = 0x2
	wsOpClose        = 0x8
	wsOpPing         = 0x9
	wsOpPong         = 0xA
)

// wsConn is a server-side WebSocket connection.
//
// Only the minimal server-side set is implemented: handshake, framed
// send/receive, ping/pong, close; no compression or fragmentation
// extensions; client-fragmented messages are transparently reassembled.
type wsConn struct {
	conn net.Conn
	br   *bufio.Reader

	// Write lock: event frames (session loop), pong/close (read
	// loop), and keepalive pings are written concurrently from
	// different goroutines; net.Conn.Write is itself safe for
	// concurrent use, but interleaving SetWriteDeadline with Write
	// can tear frames, so writeFrame serializes them uniformly
	// under this lock.
	wmu sync.Mutex
}

// wsUpgrade completes the HTTP upgrade handshake.
// conn: the already-hijacked connection.
// key: the request's Sec-WebSocket-Key.
// returns: the ready WebSocket connection.
func wsUpgrade(conn net.Conn, br *bufio.Reader, key string) (*wsConn, error) {
	if key == "" {
		return nil, errors.New("missing Sec-WebSocket-Key")
	}
	sum := sha1.Sum([]byte(key + wsGUID))
	accept := base64.StdEncoding.EncodeToString(sum[:])
	resp := "HTTP/1.1 101 Switching Protocols\r\n" +
		"Upgrade: websocket\r\n" +
		"Connection: Upgrade\r\n" +
		"Sec-WebSocket-Accept: " + accept + "\r\n\r\n"
	if _, err := conn.Write([]byte(resp)); err != nil {
		return nil, fmt.Errorf("write upgrade response: %w", err)
	}
	return &wsConn{conn: conn, br: br}, nil
}

// wsMaxMessage is the per-message accumulation cap.
//
// A single frame has a 16MB cap, but fragments with FIN=0 can
// accumulate without bound, so the cap must apply to the "total
// accumulated so far" — otherwise an unauthenticated endpoint can be
// deterministically OOMed.
const wsMaxMessage = 16 << 20

// wsErrMessageTooBig is the fragment-accumulation-over-limit error;
// callers should reply with a 1009 close.
var wsErrMessageTooBig = errors.New("websocket message exceeds limit")

// ReadMessage reads the next complete message, handling ping/pong and
// fragment reassembly internally.
// returns: the opcode (text/binary) and payload; an error when the
// connection closes.
func (ws *wsConn) ReadMessage() (int, []byte, error) {
	var assembled []byte
	resultOp := 0
	// In-fragment marker: control frames may interleave between
	// fragments; data frames may not.
	inFragment := false
	for {
		op, payload, fin, err := ws.readFrame()
		if err != nil {
			return 0, nil, err
		}
		// RFC6455 §5.5: control frames must not be fragmented
		// (FIN must be 1) and payloads are capped at 125 bytes.
		if op >= wsOpClose && (!fin || len(payload) > 125) {
			return 0, nil, wsErrProtocol
		}
		switch op {
		case wsOpPing:
			// The protocol requires replying pong promptly,
			// echoing the payload back verbatim.
			if err := ws.writeFrame(wsOpPong, payload); err != nil {
				return 0, nil, err
			}
		case wsOpPong:
			// Keepalive pong: the frame's arrival already
			// refreshed the read window inside readFrame;
			// nothing more to do.
		case wsOpClose:
			_ = ws.writeFrame(wsOpClose, payload)
			return 0, nil, io.EOF
		case wsOpContinuation:
			// A continuation frame with no fragment in flight
			// is a protocol violation.
			if !inFragment {
				return 0, nil, wsErrProtocol
			}
			assembled = append(assembled, payload...)
			if len(assembled) > wsMaxMessage {
				return 0, nil, wsErrMessageTooBig
			}
			if fin {
				return resultOp, assembled, nil
			}
		default:
			// A new first data frame arriving while the
			// previous fragmented message is unfinished must
			// not silently overwrite the accumulated content;
			// reject it as a protocol violation.
			if inFragment {
				return 0, nil, wsErrProtocol
			}
			resultOp = op
			assembled = payload
			if fin {
				return op, assembled, nil
			}
			inFragment = true
		}
	}
}

// WriteText sends a text frame.
// returns: any write failure error.
func (ws *wsConn) WriteText(data []byte) error {
	return ws.writeFrame(wsOpText, data)
}

// WriteCloseStatus sends a close frame carrying a status code.
// code: an RFC6455 close status code, e.g. 1009 for message too large.
// returns: any write failure error.
func (ws *wsConn) WriteCloseStatus(code int) error {
	payload := []byte{byte(code >> 8), byte(code)}
	return ws.writeFrame(wsOpClose, payload)
}

// Close closes the underlying connection.
func (ws *wsConn) Close() error { return ws.conn.Close() }

// wsPingInterval is the server keepalive ping period.
//
// Idle connections send empty-payload pings on this cadence: a client
// pong (or any frame it sends) refreshes the read window inside
// readFrame, so active connections are never killed by the 2-minute
// window in error; for an already-dead peer, a write failure triggers
// teardown and the read window reclaims the connection as a fallback,
// leaving no zombie connections.
const wsPingInterval = 60 * time.Second

// keepalive sends pings periodically to keep the connection alive.
//
// It exits as soon as ctx is canceled (connection teardown), so no
// goroutine leaks; a write failure means the peer is unreachable, so
// it proactively cancels the connection via onDead instead of idly
// waiting out the read timeout.
func (ws *wsConn) keepalive(ctx context.Context, onDead func()) {
	ticker := time.NewTicker(wsPingInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// The write lock is held only for the instant the
			// frame is written; while waiting on the ticker it
			// does not block other write paths such as
			// pong/close, so no deadlock is possible.
			if err := ws.writeFrame(wsOpPing, nil); err != nil {
				if onDead != nil {
					onDead()
				}
				return
			}
		}
	}
}

// wsErrProtocol signals a protocol violation (unmasked frame / illegal
// control frame / fragment disorder); callers should reply with a 1002 close.
var wsErrProtocol = errors.New("websocket protocol violation")

// Frame-level timeout windows.
//
// The read window refreshes on "frame start": a slow-drip attack
// (frames that never complete) gets cut off by the window, while a
// normal session keeps renewing as long as every frame arrives within
// the window, unaffected by long-running server tasks; for an idle
// client, the pong it replies to a keepalive ping is also a frame and
// likewise renews the window.
const (
	wsReadWindow  = 2 * time.Minute
	wsWriteWindow = 30 * time.Second
)

// readFrame reads a single frame and unmasks it.
// returns: the opcode, payload, and whether FIN is set.
func (ws *wsConn) readFrame() (int, []byte, bool, error) {
	// Refresh the read window on every frame; slowloris-style
	// slow connections are eventually cut off by the timeout.
	_ = ws.conn.SetReadDeadline(time.Now().Add(wsReadWindow))
	var hdr [2]byte
	if _, err := io.ReadFull(ws.br, hdr[:]); err != nil {
		return 0, nil, false, err
	}
	fin := hdr[0]&0x80 != 0
	op := int(hdr[0] & 0x0F)
	masked := hdr[1]&0x80 != 0
	if !masked {
		// RFC6455 §5.1: client frames must be masked;
		// violations fail the connection with 1002.
		return 0, nil, false, wsErrProtocol
	}
	length := uint64(hdr[1] & 0x7F)
	switch length {
	case 126:
		var ext [2]byte
		if _, err := io.ReadFull(ws.br, ext[:]); err != nil {
			return 0, nil, false, err
		}
		length = uint64(binary.BigEndian.Uint16(ext[:]))
	case 127:
		var ext [8]byte
		if _, err := io.ReadFull(ws.br, ext[:]); err != nil {
			return 0, nil, false, err
		}
		length = binary.BigEndian.Uint64(ext[:])
	}
	// Anti-abuse cap: 16MB per frame.
	if length > 16<<20 {
		return 0, nil, false, errors.New("frame too large")
	}
	var mask [4]byte
	if _, err := io.ReadFull(ws.br, mask[:]); err != nil {
		return 0, nil, false, err
	}
	payload := make([]byte, length)
	if _, err := io.ReadFull(ws.br, payload); err != nil {
		return 0, nil, false, err
	}
	for i := range payload {
		payload[i] ^= mask[i%4]
	}
	return op, payload, fin, nil
}

// writeFrame writes a server frame (unmasked).
//
// All frame writes (events/pong/close/keepalive ping) are serialized
// through here: concurrent callers may only interleave at "whole
// frame" granularity — interleaving SetWriteDeadline with Write
// would tear frames.
func (ws *wsConn) writeFrame(op int, data []byte) error {
	ws.wmu.Lock()
	defer ws.wmu.Unlock()
	// The write window prevents a peer that stops reading from
	// blocking writes and pinning the connection.
	_ = ws.conn.SetWriteDeadline(time.Now().Add(wsWriteWindow))
	n := len(data)
	frame := []byte{byte(0x80 | op)}
	switch {
	case n < 126:
		frame = append(frame, byte(n))
	case n < 1<<16:
		frame = append(frame, 126, byte(n>>8), byte(n))
	default:
		ext := make([]byte, 8)
		binary.BigEndian.PutUint64(ext, uint64(n))
		frame = append(frame, 127)
		frame = append(frame, ext...)
	}
	frame = append(frame, data...)
	_, err := ws.conn.Write(frame)
	return err
}

// wsWriteJSON serializes and writes a text frame.
func wsWriteJSON(ws *wsConn, v any) error {
	payload, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return ws.WriteText(payload)
}

// wsHeaderContains performs lenient case-insensitive matching for the
// Connection/Upgrade header.
// returns: true if the target token is present.
func wsHeaderContains(header, token string) bool {
	for _, part := range strings.Split(header, ",") {
		if strings.EqualFold(strings.TrimSpace(part), token) {
			return true
		}
	}
	return false
}
