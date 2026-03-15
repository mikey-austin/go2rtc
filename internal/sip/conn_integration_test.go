package sip

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/AlexxIT/go2rtc/internal/streams"
	"github.com/AlexxIT/go2rtc/pkg/core"
	"github.com/emiago/sipgo"
	sipmsg "github.com/emiago/sipgo/sip"
	"github.com/pion/rtp"
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
		addr, codec, err := ParseOffer(req.Body(), []*core.Codec{
			{Name: core.CodecPCMU, ClockRate: 8000, PayloadType: 0},
		})
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

func TestConnAcceptsInboundInviteAndRTPBridge(t *testing.T) {
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

	require.NoError(t, manager.ensureServer())

	streamName := "sip-inbound-doorbell"
	source := newTestAudioSource()
	streams.HandleFunc("testsip-inbound", func(string) (core.Producer, error) {
		return source, nil
	})

	_, err := streams.New(streamName, "testsip-inbound:"+streamName)
	require.NoError(t, err)

	t.Cleanup(func() {
		streams.Delete(streamName)
		_ = source.Stop()
	})

	clientRTP, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 0})
	require.NoError(t, err)
	defer clientRTP.Close()

	clientPackets := make(chan *rtp.Packet, 1)
	go readRTPPackets(clientRTP, clientPackets)

	offerCodec := &core.Codec{Name: core.CodecPCMU, ClockRate: 8000, PayloadType: 0}
	offer, err := BuildOffer("127.0.0.1", clientRTP.LocalAddr().(*net.UDPAddr).Port, []*core.Codec{offerCodec})
	require.NoError(t, err)

	ua, err := sipgo.NewUA(sipgo.WithUserAgent("sip-test"))
	require.NoError(t, err)
	defer ua.Close()

	client, err := sipgo.NewClient(ua, sipgo.WithClientConnectionAddr("127.0.0.1:0"))
	require.NoError(t, err)

	dialogs := sipgo.NewDialogClientCache(client, sipmsg.ContactHeader{})
	dstHost := manager.listenAddr.IP.String()
	if dstHost == "" || dstHost == "0.0.0.0" || dstHost == "::" {
		dstHost = "127.0.0.1"
	}

	req := sipmsg.NewRequest(sipmsg.INVITE, sipmsg.Uri{
		Scheme: "sip",
		User:   streamName,
		Host:   dstHost,
		Port:   manager.listenAddr.Port,
	})
	req.SetTransport("UDP")
	req.SetBody(offer)
	req.AppendHeader(&sipmsg.ContactHeader{
		Address: sipmsg.Uri{
			Scheme: "sip",
			User:   "caller",
			Host:   "127.0.0.1",
			Port:   5099,
		},
	})
	req.AppendHeader(sipmsg.NewHeader("Content-Type", "application/sdp"))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	dialog, err := dialogs.WriteInvite(ctx, req)
	require.NoError(t, err)
	defer dialog.Close()

	require.NoError(t, dialog.WaitAnswer(ctx, sipgo.AnswerOptions{}))
	require.NoError(t, dialog.Ack(context.Background()))

	remoteRTP, codec, err := ParseAnswer(dialog.InviteResponse.Body(), []*core.Codec{offerCodec})
	require.NoError(t, err)
	require.Equal(t, core.CodecPCMU, codec.Name)

	clientPacket := &rtp.Packet{
		Header: rtp.Header{
			Version:        2,
			PayloadType:    codec.PayloadType,
			SequenceNumber: 111,
			Timestamp:      160,
			SSRC:           555,
		},
		Payload: []byte{1, 2, 3, 4},
	}
	data, err := clientPacket.Marshal()
	require.NoError(t, err)

	_, err = clientRTP.WriteToUDP(data, remoteRTP)
	require.NoError(t, err)

	select {
	case packet := <-source.micPackets:
		require.Equal(t, clientPacket.Payload, packet.Payload)
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting stream backchannel RTP")
	}

	require.Eventually(t, func() bool {
		return source.cameraTrack() != nil
	}, 3*time.Second, 20*time.Millisecond)

	cameraPacket := &rtp.Packet{
		Header: rtp.Header{
			Version:        2,
			PayloadType:    codec.PayloadType,
			SequenceNumber: 222,
			Timestamp:      320,
			SSRC:           777,
		},
		Payload: []byte{9, 8, 7, 6},
	}
	source.cameraTrack().WriteRTP(cameraPacket)

	select {
	case packet := <-clientPackets:
		require.Equal(t, cameraPacket.Payload, packet.Payload)
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting caller RTP")
	}

	require.NoError(t, dialog.Bye(context.Background()))
	require.Eventually(t, func() bool {
		return inboundDialogsLen(manager) == 0
	}, 3*time.Second, 20*time.Millisecond)
}

func buildAnswer(localIP string, localPort int, codec *core.Codec) ([]byte, error) {
	return BuildOffer(localIP, localPort, []*core.Codec{codec})
}

type testAudioSource struct {
	medias     []*core.Media
	mu         sync.RWMutex
	camera     *core.Receiver
	senders    []*core.Sender
	micPackets chan *rtp.Packet
	done       chan struct{}
	closeOnce  sync.Once
}

func newTestAudioSource() *testAudioSource {
	codec := &core.Codec{Name: core.CodecPCMU, ClockRate: 8000, PayloadType: 0}
	return &testAudioSource{
		medias: []*core.Media{
			{Kind: core.KindAudio, Direction: core.DirectionRecvonly, Codecs: []*core.Codec{codec.Clone()}},
			{Kind: core.KindAudio, Direction: core.DirectionSendonly, Codecs: []*core.Codec{codec.Clone()}},
		},
		micPackets: make(chan *rtp.Packet, 1),
		done:       make(chan struct{}),
	}
}

func (s *testAudioSource) GetMedias() []*core.Media {
	return s.medias
}

func (s *testAudioSource) GetTrack(media *core.Media, codec *core.Codec) (*core.Receiver, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.camera != nil {
		return s.camera, nil
	}

	s.camera = core.NewReceiver(media, codec.Clone())
	return s.camera, nil
}

func (s *testAudioSource) AddTrack(media *core.Media, codec *core.Codec, track *core.Receiver) error {
	sender := core.NewSender(media, codec.Clone())
	sender.Handler = func(packet *rtp.Packet) {
		clone := *packet
		clone.Payload = append([]byte(nil), packet.Payload...)
		s.micPackets <- &clone
	}
	sender.HandleRTP(track)

	s.mu.Lock()
	s.senders = append(s.senders, sender)
	s.mu.Unlock()

	return nil
}

func (s *testAudioSource) Start() error {
	<-s.done
	return nil
}

func (s *testAudioSource) Stop() error {
	s.mu.Lock()
	senders := append([]*core.Sender(nil), s.senders...)
	camera := s.camera
	s.senders = nil
	s.camera = nil
	s.mu.Unlock()

	for _, sender := range senders {
		sender.Close()
		sender.Wait()
	}
	if camera != nil {
		camera.Close()
	}
	s.closeOnce.Do(func() {
		close(s.done)
	})
	return nil
}

func (s *testAudioSource) cameraTrack() *core.Receiver {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.camera
}

func readRTPPackets(conn *net.UDPConn, out chan<- *rtp.Packet) {
	buf := make([]byte, 1500)
	for {
		n, _, err := conn.ReadFromUDP(buf)
		if err != nil {
			return
		}

		packet := &rtp.Packet{}
		if err = packet.Unmarshal(buf[:n]); err != nil {
			continue
		}

		clone := *packet
		clone.Payload = append([]byte(nil), packet.Payload...)
		out <- &clone
	}
}

func inboundDialogsLen(m *Manager) int {
	n := 0
	m.inbound.Range(func(_, _ any) bool {
		n++
		return true
	})
	return n
}
