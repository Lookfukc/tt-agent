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

// wsGUID RFC6455 握手魔数
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

// wsConn 服务端 WebSocket 连接
//
// 只实现服务端最小集：握手、分帧收发、ping/pong、close；
// 无压缩无分片扩展，客户端分片消息做透明拼接
type wsConn struct {
	conn net.Conn
	br   *bufio.Reader

	// 写锁：事件帧（会话循环）、pong/close（读循环）与保活 ping
	// 来自不同 goroutine 并发写出；net.Conn.Write 本身并发安全，
	// 但 SetWriteDeadline 与 Write 交错会产生撕裂帧，
	// 统一在 writeFrame 内持锁串行化
	wmu sync.Mutex
}

// wsUpgrade 完成 HTTP 升级握手
// conn: 已劫持的连接
// key: 请求的 Sec-WebSocket-Key
// returns: 就绪的 WebSocket 连接
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

// wsMaxMessage 单条消息累计上限
//
// 单帧有 16MB 上限，但 FIN=0 的分片可无限累计，
// 上限必须作用在"已累计总量"上，否则无认证端点可被确定性 OOM
const wsMaxMessage = 16 << 20

// wsErrMessageTooBig 分片累计超限错误，调用方应回 1009 关闭
var wsErrMessageTooBig = errors.New("websocket message exceeds limit")

// ReadMessage 读下一条完整消息，内部处理 ping/pong 与分片拼接
// returns: opcode（text/binary）与载荷；连接关闭时返回错误
func (ws *wsConn) ReadMessage() (int, []byte, error) {
	var assembled []byte
	resultOp := 0
	// 分片在途标记：控制帧可在分片间穿插，数据帧不容许
	inFragment := false
	for {
		op, payload, fin, err := ws.readFrame()
		if err != nil {
			return 0, nil, err
		}
		// RFC6455 §5.5：控制帧不容许分片（必须 FIN=1）且载荷 ≤125 字节
		if op >= wsOpClose && (!fin || len(payload) > 125) {
			return 0, nil, wsErrProtocol
		}
		switch op {
		case wsOpPing:
			// 协议要求尽快回 pong，载荷原样带回
			if err := ws.writeFrame(wsOpPong, payload); err != nil {
				return 0, nil, err
			}
		case wsOpPong:
			// 保活 pong：帧到达即在 readFrame 内刷新了读窗口，无需处理
		case wsOpClose:
			_ = ws.writeFrame(wsOpClose, payload)
			return 0, nil, io.EOF
		case wsOpContinuation:
			// 无在途分片却收到 continuation 属协议违规
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
			// 上一条分片消息未收完又来了新的首个数据帧，
			// 不能静默覆盖已累计内容，按协议违规拒绝
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

// WriteText 发送文本帧
// returns: 写失败错误
func (ws *wsConn) WriteText(data []byte) error {
	return ws.writeFrame(wsOpText, data)
}

// WriteCloseStatus 发送带状态码的关闭帧
// code: RFC6455 关闭状态码，如 1009 消息过大
// returns: 写失败错误
func (ws *wsConn) WriteCloseStatus(code int) error {
	payload := []byte{byte(code >> 8), byte(code)}
	return ws.writeFrame(wsOpClose, payload)
}

// Close 关闭底层连接
func (ws *wsConn) Close() error { return ws.conn.Close() }

// wsPingInterval 服务端保活 ping 周期
//
// 空闲连接按此周期发送空载荷 ping：客户端回 pong（或发出任何帧）
// 都会在 readFrame 里刷新读窗口，活跃连接不再被 2 分钟窗口误杀；
// 对已死对端，写失败触发收尾，读窗口兜底回收，不留僵尸连接
const wsPingInterval = 60 * time.Second

// keepalive 周期发送 ping 保活
//
// ctx 取消（连接收尾）即退出，杜绝 goroutine 泄漏；写失败说明
// 对端已不可达，经 onDead 主动取消连接而不是干等读超时
func (ws *wsConn) keepalive(ctx context.Context, onDead func()) {
	ticker := time.NewTicker(wsPingInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// 写锁只在帧写出瞬间持有，ticker 等待期间不阻塞
			// pong/close 等其它写入路径，无死锁可能
			if err := ws.writeFrame(wsOpPing, nil); err != nil {
				if onDead != nil {
					onDead()
				}
				return
			}
		}
	}
}

// wsErrProtocol 协议违规（未掩码帧/非法控制帧/分片错乱），调用方应回 1002 关闭
var wsErrProtocol = errors.New("websocket protocol violation")

// 帧级超时窗口
//
// 读窗口按"帧开始"刷新：慢速滴字攻击（帧永不完整）会被窗口掐断，
// 正常会话只要每帧在窗口内到达即持续续期，不受服务端长任务影响；
// 空闲客户端对保活 ping 回的 pong 也是帧，同样续期
const (
	wsReadWindow  = 2 * time.Minute
	wsWriteWindow = 30 * time.Second
)

// readFrame 读单个帧并解掩码
// returns: opcode、载荷、是否 FIN
func (ws *wsConn) readFrame() (int, []byte, bool, error) {
	// 每帧刷新读窗口，slowloris 式慢速连接最终被超时掐断
	_ = ws.conn.SetReadDeadline(time.Now().Add(wsReadWindow))
	var hdr [2]byte
	if _, err := io.ReadFull(ws.br, hdr[:]); err != nil {
		return 0, nil, false, err
	}
	fin := hdr[0]&0x80 != 0
	op := int(hdr[0] & 0x0F)
	masked := hdr[1]&0x80 != 0
	if !masked {
		// RFC6455 §5.1：客户端帧必须掩码，违规以 1002 失败连接
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
	// 上限防滥用：单帧 16MB
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

// writeFrame 写服务端帧（不带掩码）
//
// 所有帧写出（事件/pong/close/保活 ping）都经此串行化：
// 并发调用只允许交错在"整帧"粒度上，SetWriteDeadline 与 Write
// 的交错会撕裂帧
func (ws *wsConn) writeFrame(op int, data []byte) error {
	ws.wmu.Lock()
	defer ws.wmu.Unlock()
	// 写窗口防对端停读导致写阻塞占住连接
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

// wsWriteJSON 序列化并写文本帧
func wsWriteJSON(ws *wsConn, v any) error {
	payload, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return ws.WriteText(payload)
}

// wsHeaderContains Connection/Upgrade 头的宽松大小写匹配
// returns: true 表示包含目标 token
func wsHeaderContains(header, token string) bool {
	for _, part := range strings.Split(header, ",") {
		if strings.EqualFold(strings.TrimSpace(part), token) {
			return true
		}
	}
	return false
}
