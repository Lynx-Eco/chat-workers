// SPDX-License-Identifier: AGPL-3.0-or-later

package wire

import (
	"bytes"
	"context"
	"encoding/binary"
	"net"
	"testing"
	"time"

	"lynx.invalid/ingest/chatworker/contract"
)

// TestConnectClearsDroppedInitKey: on its error paths Connect drops the Init
// it read, and the store key in it is zeroed first. The frame is read into
// one the test keeps (connect), so the dropped Init can be looked at:
//   - a contract version it does not speak: the whole Init decoded, its key
//     zeroed by Connect;
//   - an Init followed by bytes that do not decode: the key already decoded
//     is zeroed by ReadFrame (zeroBytes) and again by Connect, so this case
//     fails only when both clears are removed.
func TestConnectClearsDroppedInitKey(t *testing.T) {
	key := bytes.Repeat([]byte{0xa5}, 32)
	frameOf := func(payload []byte) []byte {
		f := binary.BigEndian.AppendUint32(nil, Magic)
		f = binary.BigEndian.AppendUint16(f, HostOpcode)
		f = append(f, CodecProtobuf, CompressionNone)
		f = binary.BigEndian.AppendUint32(f, uint32(len(payload)))
		return append(f, payload...)
	}
	initOf := func(version uint32) []byte {
		b, err := contract.Marshal(&contract.HostFrame{Body: &contract.HostFrame_Init{Init: &contract.Init{
			ContractVersion: version, StoreKey: bytes.Clone(key)}}})
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	for _, c := range []struct {
		name  string
		frame []byte
		kind  Kind
	}{
		{"unsupported version", frameOf(initOf(contract.Version + 1)), KindUnsupportedContractVersion},
		{"half decoded", frameOf(append(initOf(contract.Version), 0x0a, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01)), KindDecode},
	} {
		t.Run(c.name, func(t *testing.T) {
			worker, host := net.Pipe()
			defer worker.Close()
			defer host.Close()
			go func() { _, _ = host.Write(c.frame) }()
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			var first contract.HostFrame
			init, hs, err := connect(ctx, worker, DefaultConfig(), &first)
			if init != nil || hs != nil || KindOf(err) != c.kind {
				t.Fatalf("connect: %v, %v, %v; want the error kind %d", init, hs, err, c.kind)
			}
			k := first.GetInit().GetStoreKey()
			if len(k) == 0 {
				t.Fatal("the dropped Init holds no key: the case is not exercised")
			}
			if !bytes.Equal(k, make([]byte, len(k))) {
				t.Fatalf("the Init Connect dropped still holds the key %x", k)
			}
		})
	}
}
