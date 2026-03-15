package sip

import (
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/AlexxIT/go2rtc/pkg/core"
	"github.com/emiago/sipgo"
	sipmsg "github.com/emiago/sipgo/sip"
	"github.com/pion/rtp"
	"github.com/pion/sdp/v3"
	"github.com/stretchr/testify/require"
)

func TestConnInviteAndRTPBridge(t *testing.T) {
	oldManager := manager
	oldCalls := calls
	manager = &Manager{listen: "127.0.0.1:0", timeout: 5 * time.Second}
	calls = sync.Map{}

	t.Cleanup(func() {
		if manager != nil {
			if manager.listener != nil {
				_ = manager.listener.Close()
			}
			if manager.ua != nil {
				_ = manager.ua.Close()
			}
		}
		manager = oldManager
		calls = oldCalls
	})

	server := startTestSIPServer(t)
	defer server.rtpConn.Close()

	conn, err := manager.newConn("sip:doorbell@" + server.addr)
	require.NoError(t, err)

	inCodec := &core.Codec{Name: core.CodecPCMU, ClockRate: 8000, PayloadType: 0}
	inTrack, err := conn.GetTrack(conn.Medias[1], inCodec)
	require.NoError(t, err)

	inPackets := make(chan *rtp.Packet, 1)
	inSender := core.NewSender(conn.Medias[1], inCodec)
	inSender.Handler = func(packet *rtp.Packet) {
		clone := *packet
		clone.Payload = append([]byte(nil), packet.Payload...)
		inPackets <- &clone
	}
	inSender.HandleRTP(inTrack)
	defer func() {
		inSender.Close()
		inSender.Wait()
	}()

	outTrack := core.NewReceiver(conn.Medias[0], inCodec)
	require.NoError(t, conn.AddTrack(conn.Medias[0], inCodec, outTrack))

	errCh := make(chan error, 1)
	go func() {
		errCh <- conn.Start()
	}()

	var callerRTP *net.UDPAddr
	select {
	case callerRTP = <-server.callerRTP:
	case err = <-server.errs:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting caller RTP address")
	}

	require.Eventually(t, func() bool {
		conn.mu.RLock()
		defer conn.mu.RUnlock()
		return conn.rtpConn != nil && conn.codec != nil && conn.remoteAddr != nil
	}, 5*time.Second, 20*time.Millisecond)

	select {
	case <-server.acked:
	case err = <-server.errs:
		require.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for ACK")
	}

	serverPacket := &rtp.Packet{
		Header: rtp.Header{
			Version:        2,
			PayloadType:    0,
			SequenceNumber: 100,
			Timestamp:      160,
			SSRC:           1234,
		},
		Payload: []byte{1, 2, 3, 4},
	}
	data, err := serverPacket.Marshal()
	require.NoError(t, err)

	_, err = server.rtpConn.WriteToUDP(data, callerRTP)
	require.NoError(t, err)

	select {
	case packet := <-inPackets:
		require.Equal(t, serverPacket.Payload, packet.Payload)
	case err = <-server.errs:
		require.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting inbound RTP")
	}

	outPacket := &rtp.Packet{
		Header: rtp.Header{
			Version:        2,
			PayloadType:    0,
			SequenceNumber: 200,
			Timestamp:      320,
			SSRC:           4321,
		},
		Payload: []byte{9, 8, 7, 6},
	}
	outTrack.WriteRTP(outPacket)

	select {
	case packet := <-server.received:
		require.Equal(t, outPacket.Payload, packet.Payload)
	case err = <-server.errs:
		require.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting outbound RTP")
	}

	require.NoError(t, conn.Stop())

	select {
	case <-server.byed:
	case err = <-server.errs:
		require.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for BYE")
	}

	select {
	case err = <-errCh:
		require.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting conn.Start shutdown")
	}
}

type testServer struct {
	rtpConn   *net.UDPConn
	addr      string
	callerRTP chan *net.UDPAddr
	acked     chan struct{}
	byed      chan struct{}
	received  chan *rtp.Packet
	errs      chan error
}

func startTestSIPServer(t *testing.T) *testServer {
	t.Helper()

	ua, err := sipgo.NewUA(sipgo.WithUserAgent("sip-test"))
	require.NoError(t, err)

	srv, err := sipgo.NewServer(ua)
	require.NoError(t, err)

	sipConn, err := net.ListenPacket("udp", "127.0.0.1:0")
	require.NoError(t, err)

	serverRTP, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 0})
	require.NoError(t, err)

	ts := &testServer{
		rtpConn:   serverRTP,
		addr:      sipConn.LocalAddr().String(),
		callerRTP: make(chan *net.UDPAddr, 1),
		acked:     make(chan struct{}, 1),
		byed:      make(chan struct{}, 1),
		received:  make(chan *rtp.Packet, 1),
		errs:      make(chan error, 4),
	}

	t.Cleanup(func() {
		_ = sipConn.Close()
		_ = serverRTP.Close()
		_ = ua.Close()
	})

	go func() {
		buf := make([]byte, 1500)
		for {
			n, _, err := serverRTP.ReadFromUDP(buf)
			if err != nil {
				return
			}

			packet := &rtp.Packet{}
			if err = packet.Unmarshal(buf[:n]); err != nil {
				continue
			}

			clone := *packet
			clone.Payload = append([]byte(nil), packet.Payload...)
			ts.received <- &clone
		}
	}()

	srv.OnInvite(func(req *sipmsg.Request, tx sipmsg.ServerTransaction) {
		addr, codec, err := parseOffer(req.Body())
		if err != nil {
			ts.errs <- err
			return
		}
		if codec.Name != core.CodecPCMU {
			ts.errs <- errors.New("unexpected negotiated codec")
			return
		}
		ts.callerRTP <- addr

		answer, err := buildAnswer("127.0.0.1", serverRTP.LocalAddr().(*net.UDPAddr).Port, codec)
		if err != nil {
			ts.errs <- err
			return
		}

		res := sipmsg.NewResponseFromRequest(req, 200, "OK", answer)
		res.AppendHeader(sipmsg.NewHeader("Content-Type", "application/sdp"))
		res.AppendHeader(&sipmsg.ContactHeader{
			Address: sipmsg.Uri{
				Scheme: "sip",
				User:   "doorbell",
				Host:   "127.0.0.1",
				Port:   sipConn.LocalAddr().(*net.UDPAddr).Port,
			},
		})
		if err = tx.Respond(res); err != nil {
			ts.errs <- err
		}
	})

	srv.OnAck(func(req *sipmsg.Request, tx sipmsg.ServerTransaction) {
		ts.acked <- struct{}{}
	})

	srv.OnBye(func(req *sipmsg.Request, tx sipmsg.ServerTransaction) {
		ts.byed <- struct{}{}
		res := sipmsg.NewResponseFromRequest(req, 200, "OK", nil)
		if err := tx.Respond(res); err != nil {
			ts.errs <- err
		}
	})

	go func() {
		_ = srv.ServeUDP(sipConn)
	}()

	return ts
}

func parseOffer(body []byte) (*net.UDPAddr, *core.Codec, error) {
	sd := &sdp.SessionDescription{}
	if err := sd.Unmarshal(body); err != nil {
		return nil, nil, err
	}

	for _, md := range sd.MediaDescriptions {
		if md.MediaName.Media != "audio" || md.MediaName.Port.Value == 0 {
			continue
		}

		media := core.UnmarshalMedia(md)
		for _, codec := range media.Codecs {
			if codec.Name != core.CodecPCMU {
				continue
			}

			host := ""
			if md.ConnectionInformation != nil && md.ConnectionInformation.Address != nil {
				host = md.ConnectionInformation.Address.Address
			}
			if host == "" && sd.ConnectionInformation != nil && sd.ConnectionInformation.Address != nil {
				host = sd.ConnectionInformation.Address.Address
			}

			addr, err := net.ResolveUDPAddr("udp", net.JoinHostPort(host, "0"))
			if err != nil {
				return nil, nil, err
			}
			addr.Port = md.MediaName.Port.Value
			return addr, codec, nil
		}
	}

	return nil, nil, errors.New("missing audio PCMU offer")
}

func buildAnswer(localIP string, localPort int, codec *core.Codec) ([]byte, error) {
	return BuildOffer(localIP, localPort, []*core.Codec{codec})
}
