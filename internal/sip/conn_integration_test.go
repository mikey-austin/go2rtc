package sip

import (
	"context"
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

var (
	testAudioCodec = &core.Codec{Name: core.CodecPCMU, ClockRate: 8000, PayloadType: 0}
	testVideoCodec = &core.Codec{
		Name:        core.CodecH264,
		ClockRate:   90000,
		PayloadType: 96,
		FmtpLine:    "packetization-mode=1;profile-level-id=42e01f;sprop-parameter-sets=Z0LgHtoCgPaE,aM4G4g==",
	}
)

func TestConnInviteAndRTPBridge(t *testing.T) {
	oldManager := manager
	oldCalls := calls
	manager = &Manager{listen: "127.0.0.1:0", timeout: 5 * time.Second}
	calls = sync.Map{}

	t.Cleanup(func() {
		closeManager(manager)
		manager = oldManager
		calls = oldCalls
	})

	server := startTestSIPServer(t)

	conn, err := manager.newConn("sip:doorbell@" + server.addr)
	require.NoError(t, err)
	conn.displayName = "Front Doorbell"

	audioInTrack, err := conn.GetTrack(conn.Medias[1], testAudioCodec)
	require.NoError(t, err)

	audioInPackets := make(chan *rtp.Packet, 1)
	audioInSender := core.NewSender(conn.Medias[1], testAudioCodec)
	audioInSender.Handler = func(packet *rtp.Packet) {
		audioInPackets <- clonePacket(packet)
	}
	audioInSender.HandleRTP(audioInTrack)
	defer func() {
		audioInSender.Close()
		audioInSender.Wait()
	}()

	audioOutTrack := core.NewReceiver(conn.Medias[0], testAudioCodec)
	require.NoError(t, conn.AddTrack(conn.Medias[0], testAudioCodec, audioOutTrack))

	videoOutTrack := core.NewReceiver(conn.Medias[2], testVideoCodec)
	require.NoError(t, conn.AddTrack(conn.Medias[2], testVideoCodec, videoOutTrack))

	errCh := make(chan error, 1)
	go func() {
		errCh <- conn.Start()
	}()

	require.Eventually(t, func() bool {
		conn.mu.RLock()
		defer conn.mu.RUnlock()
		return conn.sessions[core.KindAudio] != nil && conn.sessions[core.KindVideo] != nil
	}, 5*time.Second, 20*time.Millisecond)

	select {
	case <-server.acked:
	case err = <-server.errs:
		require.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for ACK")
	}
	require.Equal(t, "Front Doorbell", server.fromDisplayName)

	serverAudio := &rtp.Packet{
		Header: rtp.Header{
			Version:        2,
			PayloadType:    0,
			SequenceNumber: 100,
			Timestamp:      160,
			SSRC:           1234,
		},
		Payload: []byte{1, 2, 3, 4},
	}
	data, err := serverAudio.Marshal()
	require.NoError(t, err)

	_, err = server.audioRTP.WriteToUDP(data, server.remote[core.KindAudio].Addr)
	require.NoError(t, err)

	select {
	case packet := <-audioInPackets:
		require.Equal(t, serverAudio.Payload, packet.Payload)
	case err = <-server.errs:
		require.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting inbound audio RTP")
	}

	audioOut := &rtp.Packet{
		Header: rtp.Header{
			Version:        2,
			PayloadType:    0,
			SequenceNumber: 200,
			Timestamp:      320,
			SSRC:           4321,
		},
		Payload: []byte{9, 8, 7, 6},
	}
	audioOutTrack.WriteRTP(audioOut)

	select {
	case packet := <-server.audioReceived:
		require.Equal(t, audioOut.Payload, packet.Payload)
	case err = <-server.errs:
		require.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting outbound audio RTP")
	}

	videoOut := &rtp.Packet{
		Header: rtp.Header{
			Version:        2,
			PayloadType:    96,
			SequenceNumber: 300,
			Timestamp:      9000,
			SSRC:           5678,
		},
		Payload: []byte{5, 4, 3, 2},
	}
	videoOutTrack.WriteRTP(videoOut)

	select {
	case packet := <-server.videoReceived:
		require.Equal(t, videoOut.Payload, packet.Payload)
	case err = <-server.errs:
		require.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting outbound video RTP")
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

func TestConnAcceptsInboundInviteAndRTPBridge(t *testing.T) {
	oldManager := manager
	oldCalls := calls
	manager = &Manager{listen: "127.0.0.1:0", timeout: 5 * time.Second}
	calls = sync.Map{}

	t.Cleanup(func() {
		closeManager(manager)
		manager = oldManager
		calls = oldCalls
	})

	require.NoError(t, manager.ensureServer())

	streamName := "sip-inbound-doorbell"
	source := newTestAVSource()
	streams.HandleFunc("testsip-inbound", func(string) (core.Producer, error) {
		return source, nil
	})

	_, err := streams.New(streamName, "testsip-inbound:"+streamName)
	require.NoError(t, err)

	t.Cleanup(func() {
		streams.Delete(streamName)
		_ = source.Stop()
	})

	clientAudioRTP, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 0})
	require.NoError(t, err)
	defer clientAudioRTP.Close()

	clientVideoRTP, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 0})
	require.NoError(t, err)
	defer clientVideoRTP.Close()

	clientAudioPackets := make(chan *rtp.Packet, 1)
	clientVideoPackets := make(chan *rtp.Packet, 1)
	go readRTPPackets(clientAudioRTP, clientAudioPackets)
	go readRTPPackets(clientVideoRTP, clientVideoPackets)

	callerMedias := []*core.Media{
		{Kind: core.KindAudio, Direction: core.DirectionSendRecv, Codecs: []*core.Codec{testAudioCodec.Clone()}},
		{Kind: core.KindVideo, Direction: core.DirectionRecvonly, Codecs: []*core.Codec{testVideoCodec.Clone()}},
	}
	offer, err := BuildOffer("127.0.0.1", map[string]int{
		core.KindAudio: clientAudioRTP.LocalAddr().(*net.UDPAddr).Port,
		core.KindVideo: clientVideoRTP.LocalAddr().(*net.UDPAddr).Port,
	}, callerMedias)
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

	remote, err := ParseAnswer(dialog.InviteResponse.Body(), callerMedias)
	require.NoError(t, err)

	clientAudio := &rtp.Packet{
		Header: rtp.Header{
			Version:        2,
			PayloadType:    0,
			SequenceNumber: 111,
			Timestamp:      160,
			SSRC:           555,
		},
		Payload: []byte{1, 2, 3, 4},
	}
	data, err := clientAudio.Marshal()
	require.NoError(t, err)

	_, err = clientAudioRTP.WriteToUDP(data, remote[core.KindAudio].Addr)
	require.NoError(t, err)

	select {
	case packet := <-source.micPackets:
		require.Equal(t, clientAudio.Payload, packet.Payload)
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting stream backchannel RTP")
	}

	require.Eventually(t, func() bool {
		return source.audioTrack() != nil && source.videoTrack() != nil
	}, 3*time.Second, 20*time.Millisecond)

	cameraAudio := &rtp.Packet{
		Header: rtp.Header{
			Version:        2,
			PayloadType:    0,
			SequenceNumber: 222,
			Timestamp:      320,
			SSRC:           777,
		},
		Payload: []byte{9, 8, 7, 6},
	}
	source.audioTrack().WriteRTP(cameraAudio)

	select {
	case packet := <-clientAudioPackets:
		require.Equal(t, cameraAudio.Payload, packet.Payload)
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting caller audio RTP")
	}

	cameraVideo := &rtp.Packet{
		Header: rtp.Header{
			Version:        2,
			PayloadType:    96,
			SequenceNumber: 333,
			Timestamp:      9000,
			SSRC:           888,
		},
		Payload: []byte{7, 7, 7, 7},
	}
	source.videoTrack().WriteRTP(cameraVideo)

	select {
	case packet := <-clientVideoPackets:
		require.Equal(t, cameraVideo.Payload, packet.Payload)
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting caller video RTP")
	}

	require.NoError(t, dialog.Bye(context.Background()))
	require.Eventually(t, func() bool {
		return inboundDialogsLen(manager) == 0
	}, 3*time.Second, 20*time.Millisecond)
}

type testServer struct {
	audioRTP        *net.UDPConn
	videoRTP        *net.UDPConn
	addr            string
	remote          map[string]*NegotiatedMedia
	fromDisplayName string
	acked           chan struct{}
	byed            chan struct{}
	audioReceived   chan *rtp.Packet
	videoReceived   chan *rtp.Packet
	errs            chan error
}

func startTestSIPServer(t *testing.T) *testServer {
	t.Helper()

	ua, err := sipgo.NewUA(sipgo.WithUserAgent("sip-test"))
	require.NoError(t, err)

	srv, err := sipgo.NewServer(ua)
	require.NoError(t, err)

	sipConn, err := net.ListenPacket("udp", "127.0.0.1:0")
	require.NoError(t, err)

	audioRTP, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 0})
	require.NoError(t, err)

	videoRTP, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 0})
	require.NoError(t, err)

	ts := &testServer{
		audioRTP:      audioRTP,
		videoRTP:      videoRTP,
		addr:          sipConn.LocalAddr().String(),
		remote:        make(map[string]*NegotiatedMedia, 2),
		acked:         make(chan struct{}, 1),
		byed:          make(chan struct{}, 1),
		audioReceived: make(chan *rtp.Packet, 1),
		videoReceived: make(chan *rtp.Packet, 1),
		errs:          make(chan error, 4),
	}

	t.Cleanup(func() {
		_ = sipConn.Close()
		_ = audioRTP.Close()
		_ = videoRTP.Close()
		_ = ua.Close()
	})

	go readRTPPackets(audioRTP, ts.audioReceived)
	go readRTPPackets(videoRTP, ts.videoReceived)

	serverMedias := []*core.Media{
		{Kind: core.KindAudio, Direction: core.DirectionSendRecv, Codecs: []*core.Codec{testAudioCodec.Clone()}},
		{Kind: core.KindVideo, Direction: core.DirectionRecvonly, Codecs: []*core.Codec{testVideoCodec.Clone()}},
	}

	srv.OnInvite(func(req *sipmsg.Request, tx sipmsg.ServerTransaction) {
		if from := req.From(); from != nil {
			ts.fromDisplayName = from.DisplayName
		}

		answerMedias, remote, err := AnswerOffer(req.Body(), serverMedias)
		if err != nil {
			ts.errs <- err
			return
		}
		ts.remote = remote

		answer, err := BuildOffer("127.0.0.1", map[string]int{
			core.KindAudio: audioRTP.LocalAddr().(*net.UDPAddr).Port,
			core.KindVideo: videoRTP.LocalAddr().(*net.UDPAddr).Port,
		}, answerMedias)
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

type testAVSource struct {
	medias     []*core.Media
	mu         sync.RWMutex
	audio      *core.Receiver
	video      *core.Receiver
	senders    []*core.Sender
	micPackets chan *rtp.Packet
	done       chan struct{}
	closeOnce  sync.Once
}

func newTestAVSource() *testAVSource {
	return &testAVSource{
		medias: []*core.Media{
			{Kind: core.KindAudio, Direction: core.DirectionRecvonly, Codecs: []*core.Codec{testAudioCodec.Clone()}},
			{Kind: core.KindVideo, Direction: core.DirectionRecvonly, Codecs: []*core.Codec{testVideoCodec.Clone()}},
			{Kind: core.KindAudio, Direction: core.DirectionSendonly, Codecs: []*core.Codec{testAudioCodec.Clone()}},
		},
		micPackets: make(chan *rtp.Packet, 1),
		done:       make(chan struct{}),
	}
}

func (s *testAVSource) GetMedias() []*core.Media {
	return s.medias
}

func (s *testAVSource) GetTrack(media *core.Media, codec *core.Codec) (*core.Receiver, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	switch media.Kind {
	case core.KindAudio:
		if s.audio == nil {
			s.audio = core.NewReceiver(media, codec.Clone())
		}
		return s.audio, nil
	case core.KindVideo:
		if s.video == nil {
			s.video = core.NewReceiver(media, codec.Clone())
		}
		return s.video, nil
	default:
		return nil, core.ErrCantGetTrack
	}
}

func (s *testAVSource) AddTrack(media *core.Media, codec *core.Codec, track *core.Receiver) error {
	sender := core.NewSender(media, codec.Clone())
	sender.Handler = func(packet *rtp.Packet) {
		s.micPackets <- clonePacket(packet)
	}
	sender.HandleRTP(track)

	s.mu.Lock()
	s.senders = append(s.senders, sender)
	s.mu.Unlock()

	return nil
}

func (s *testAVSource) Start() error {
	<-s.done
	return nil
}

func (s *testAVSource) Stop() error {
	s.mu.Lock()
	senders := append([]*core.Sender(nil), s.senders...)
	audio := s.audio
	video := s.video
	s.senders = nil
	s.audio = nil
	s.video = nil
	s.mu.Unlock()

	for _, sender := range senders {
		sender.Close()
		sender.Wait()
	}
	if audio != nil {
		audio.Close()
	}
	if video != nil {
		video.Close()
	}
	s.closeOnce.Do(func() {
		close(s.done)
	})
	return nil
}

func (s *testAVSource) audioTrack() *core.Receiver {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.audio
}

func (s *testAVSource) videoTrack() *core.Receiver {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.video
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

		out <- clonePacket(packet)
	}
}

func clonePacket(packet *rtp.Packet) *rtp.Packet {
	clone := *packet
	clone.Payload = append([]byte(nil), packet.Payload...)
	return &clone
}

func inboundDialogsLen(m *Manager) int {
	n := 0
	m.inbound.Range(func(_, _ any) bool {
		n++
		return true
	})
	return n
}

func closeManager(m *Manager) {
	if m == nil {
		return
	}
	if m.listener != nil {
		_ = m.listener.Close()
	}
	if m.ua != nil {
		_ = m.ua.Close()
	}
}
