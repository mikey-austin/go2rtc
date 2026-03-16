package sip

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/AlexxIT/go2rtc/pkg/core"
	"github.com/AlexxIT/go2rtc/pkg/pcm"
	"github.com/emiago/sipgo"
	sipmsg "github.com/emiago/sipgo/sip"
	"github.com/pion/rtp"
)

var errSIPClosed = errors.New("sip closed")

type mediaSession struct {
	kind       string
	rtpConn    *net.UDPConn
	remoteAddr *net.UDPAddr
	codec      *core.Codec
	receive    bool
}

type Conn struct {
	core.Connection

	manager *Manager
	uri     sipmsg.Uri

	mu        sync.RWMutex
	started   bool
	stopped   bool
	clientDlg *sipgo.DialogClientSession
	serverDlg *sipgo.DialogServerSession
	onClose   func()
	cancel    context.CancelCauseFunc
	sessions  map[string]*mediaSession
	readErr   error
}

func NewConn(manager *Manager, rawURL string, uri sipmsg.Uri) *Conn {
	conn := newConn(manager, rawURL, uri.HostPort())
	conn.uri = uri
	return conn
}

func NewInboundConn(manager *Manager, req *sipmsg.Request, streamName string) *Conn {
	source := req.Recipient.String()
	if source == "" {
		source = "sip:" + streamName
	}

	conn := newConn(manager, source, req.Source())
	conn.Source = source
	conn.URL = source
	conn.RemoteAddr = req.Source()
	return conn
}

func newConn(manager *Manager, source, remote string) *Conn {
	medias := []*core.Media{
		{
			Kind:      core.KindAudio,
			Direction: core.DirectionSendonly,
			Codecs:    supportedAudioCodecs(),
		},
		{
			Kind:      core.KindAudio,
			Direction: core.DirectionRecvonly,
			Codecs:    supportedAudioCodecs(),
		},
		{
			Kind:      core.KindVideo,
			Direction: core.DirectionSendonly,
			Codecs:    supportedVideoCodecs(),
		},
	}

	return &Conn{
		Connection: core.Connection{
			ID:         core.NewID(),
			FormatName: "sip",
			Protocol:   "sip+udp",
			RemoteAddr: remote,
			Source:     source,
			URL:        source,
			Medias:     medias,
		},
		manager: manager,
	}
}

func (c *Conn) attachInbound(
	dialog *sipgo.DialogServerSession,
	sessions map[string]*mediaSession,
	remote map[string]*NegotiatedMedia,
	answer []byte,
) {
	c.mu.Lock()
	c.serverDlg = dialog
	c.sessions = sessions
	c.SDP = string(answer)
	c.mu.Unlock()

	c.applyNegotiated(remote)
}

func (c *Conn) GetTrack(media *core.Media, codec *core.Codec) (*core.Receiver, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, receiver := range c.Receivers {
		if receiver.Media.Kind == media.Kind && receiver.Codec.Match(codec) {
			return receiver, nil
		}
	}

	receiver := core.NewReceiver(media, codec.Clone())
	c.Receivers = append(c.Receivers, receiver)
	return receiver, nil
}

func (c *Conn) AddTrack(media *core.Media, codec *core.Codec, track *core.Receiver) error {
	sender := core.NewSender(media, track.Codec.Clone())
	sender.Handler = c.outboundHandler(media.Kind, track.Codec.Clone())
	sender.HandleRTP(track)

	c.mu.Lock()
	c.Senders = append(c.Senders, sender)
	c.mu.Unlock()

	return nil
}

func (c *Conn) Start() error {
	c.mu.Lock()
	if c.started {
		c.mu.Unlock()
		return nil
	}
	c.started = true
	c.mu.Unlock()

	ctx, cancel := context.WithCancelCause(context.Background())
	c.mu.Lock()
	c.cancel = cancel
	c.mu.Unlock()

	defer cancel(nil)
	defer c.close(false)

	if err := c.prepare(ctx); err != nil {
		return err
	}

	c.mu.RLock()
	sessions := cloneSessions(c.sessions)
	c.mu.RUnlock()

	readers := 0
	errCh := make(chan error, len(sessions))
	for _, session := range sessions {
		if !session.receive || session.rtpConn == nil {
			continue
		}
		readers++
		go c.readLoop(ctx, session, errCh)
	}

	if readers == 0 {
		<-ctx.Done()
		if cause := context.Cause(ctx); cause != nil && !isExpectedClose(cause) {
			return cause
		}
		return nil
	}

	for readers > 0 {
		select {
		case err := <-errCh:
			readers--
			if err == nil {
				continue
			}
			return err
		case <-ctx.Done():
			if cause := context.Cause(ctx); cause != nil && !isExpectedClose(cause) {
				return cause
			}
			return nil
		}
	}

	return nil
}

func (c *Conn) Stop() error {
	c.close(true)
	return nil
}

func (c *Conn) prepare(parent context.Context) error {
	c.mu.RLock()
	ready := len(c.sessions) > 0
	c.mu.RUnlock()

	if ready {
		return nil
	}

	return c.invite(parent)
}

func (c *Conn) invite(parent context.Context) error {
	localMedias, sessions, ports, err := c.localMedias()
	if err != nil {
		return err
	}

	hostPort := net.JoinHostPort(c.uri.Host, strconv.Itoa(defaultSIPPort(c.uri.Port)))
	contact, localIP, err := c.manager.contactHeader(hostPort)
	if err != nil {
		closeSessions(sessions)
		return err
	}

	offer, err := BuildOffer(localIP, ports, localMedias)
	if err != nil {
		closeSessions(sessions)
		return err
	}

	req := sipmsg.NewRequest(sipmsg.INVITE, c.uri)
	req.SetTransport("UDP")
	req.SetBody(offer)
	req.AppendHeader(contact)
	req.AppendHeader(sipmsg.NewHeader("Content-Type", "application/sdp"))

	ctx, cancel := context.WithTimeout(parent, c.manager.timeout)
	defer cancel()

	dialog, err := c.manager.dialogs.WriteInvite(ctx, req)
	if err != nil {
		closeSessions(sessions)
		return err
	}

	dialog.OnState(func(state sipmsg.DialogState) {
		if state == sipmsg.DialogStateEnded {
			c.close(false)
		}
	})

	if err = dialog.WaitAnswer(ctx, sipgo.AnswerOptions{}); err != nil {
		closeSessions(sessions)
		return err
	}

	if err = dialog.Ack(context.Background()); err != nil {
		closeSessions(sessions)
		return err
	}

	remote, err := ParseAnswer(dialog.InviteResponse.Body(), localMedias)
	if err != nil {
		closeSessions(sessions)
		return err
	}

	c.mu.Lock()
	c.clientDlg = dialog
	c.sessions = sessions
	c.SDP = string(dialog.InviteResponse.Body())
	c.mu.Unlock()

	c.applyNegotiated(remote)
	return nil
}

func (c *Conn) localMedias() ([]*core.Media, map[string]*mediaSession, map[string]int, error) {
	c.mu.RLock()
	senders := append([]*core.Sender(nil), c.Senders...)
	receivers := append([]*core.Receiver(nil), c.Receivers...)
	c.mu.RUnlock()

	sendAudio := collectSenderCodecs(senders, core.KindAudio)
	recvAudio := collectReceiverCodecs(receivers, core.KindAudio)
	sendVideo := collectSenderCodecs(senders, core.KindVideo)

	medias := make([]*core.Media, 0, 2)
	sessions := make(map[string]*mediaSession, 2)
	ports := make(map[string]int, 2)

	if media := buildLocalMedia(core.KindAudio, sendAudio, recvAudio); media != nil {
		session, err := newMediaSession(media.Kind)
		if err != nil {
			closeSessions(sessions)
			return nil, nil, nil, err
		}
		sessions[media.Kind] = session
		ports[media.Kind] = session.rtpConn.LocalAddr().(*net.UDPAddr).Port
		medias = append(medias, media)
	}

	if media := buildLocalMedia(core.KindVideo, sendVideo, nil); media != nil {
		session, err := newMediaSession(media.Kind)
		if err != nil {
			closeSessions(sessions)
			return nil, nil, nil, err
		}
		sessions[media.Kind] = session
		ports[media.Kind] = session.rtpConn.LocalAddr().(*net.UDPAddr).Port
		medias = append(medias, media)
	}

	if len(medias) == 0 {
		return nil, nil, nil, errors.New("sip: no compatible medias")
	}

	return medias, sessions, ports, nil
}

func (c *Conn) applyNegotiated(remote map[string]*NegotiatedMedia) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for kind, session := range c.sessions {
		negotiated, ok := remote[kind]
		if !ok {
			if session.rtpConn != nil {
				_ = session.rtpConn.Close()
			}
			delete(c.sessions, kind)
			continue
		}

		session.remoteAddr = negotiated.Addr
		session.codec = negotiated.Codec
		session.receive = mediaCanRecv(negotiated.Direction)
	}
}

func (c *Conn) readLoop(ctx context.Context, session *mediaSession, errCh chan<- error) {
	buf := make([]byte, 1500)
	for {
		n, _, err := session.rtpConn.ReadFromUDP(buf)
		if err != nil {
			if errors.Is(err, net.ErrClosed) || context.Cause(ctx) != nil {
				errCh <- nil
				return
			}
			c.mu.Lock()
			c.readErr = err
			c.mu.Unlock()
			errCh <- err
			return
		}

		packet := &rtp.Packet{}
		if err = packet.Unmarshal(buf[:n]); err != nil {
			continue
		}

		c.Recv += n
		c.dispatchInbound(session.kind, packet)
	}
}

func (c *Conn) dispatchInbound(kind string, packet *rtp.Packet) {
	c.mu.RLock()
	session := c.sessions[kind]
	receivers := append([]*core.Receiver(nil), c.Receivers...)
	c.mu.RUnlock()

	if session == nil || session.codec == nil {
		return
	}

	for _, receiver := range receivers {
		if receiver.Media.Kind != kind {
			continue
		}

		clone := *packet
		clone.PayloadType = receiver.Codec.PayloadType

		if receiver.Codec.Name != session.codec.Name {
			clone.Payload = pcm.Transcode(receiver.Codec, session.codec)(packet.Payload)
		}

		receiver.WriteRTP(&clone)
	}
}

func (c *Conn) outboundHandler(kind string, src *core.Codec) core.HandlerFunc {
	return func(packet *rtp.Packet) {
		c.mu.RLock()
		session := c.sessions[kind]
		c.mu.RUnlock()

		if session == nil || session.codec == nil || session.rtpConn == nil || session.remoteAddr == nil {
			return
		}

		clone := *packet
		clone.PayloadType = session.codec.PayloadType

		if src.Name != session.codec.Name {
			clone.Payload = pcm.Transcode(session.codec, src)(packet.Payload)
		}

		data, err := clone.Marshal()
		if err != nil {
			return
		}

		if _, err = session.rtpConn.WriteToUDP(data, session.remoteAddr); err == nil {
			c.Send += len(data)
		}
	}
}

func (c *Conn) close(sendBye bool) {
	c.mu.Lock()
	if c.stopped {
		c.mu.Unlock()
		return
	}
	c.stopped = true

	cancel := c.cancel
	clientDlg := c.clientDlg
	serverDlg := c.serverDlg
	senders := append([]*core.Sender(nil), c.Senders...)
	receivers := append([]*core.Receiver(nil), c.Receivers...)
	sessions := cloneSessions(c.sessions)
	onClose := c.onClose
	c.mu.Unlock()

	if cancel != nil {
		cancel(errSIPClosed)
	}

	if sendBye && clientDlg != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		_ = clientDlg.Bye(ctx)
		cancel()
	}

	if sendBye && serverDlg != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		_ = serverDlg.Bye(ctx)
		cancel()
	}

	closeSessions(sessions)

	for _, sender := range senders {
		sender.Close()
		sender.Wait()
	}
	for _, receiver := range receivers {
		receiver.Close()
	}

	if onClose != nil {
		onClose()
	}
}

func (c *Conn) readAck(req *sipmsg.Request, tx sipmsg.ServerTransaction) error {
	c.mu.RLock()
	dialog := c.serverDlg
	c.mu.RUnlock()

	if dialog == nil {
		return sipgo.ErrDialogDoesNotExists
	}

	return dialog.ReadAck(req, tx)
}

func (c *Conn) readBye(req *sipmsg.Request, tx sipmsg.ServerTransaction) error {
	c.mu.RLock()
	dialog := c.serverDlg
	c.mu.RUnlock()

	if dialog == nil {
		return sipgo.ErrDialogDoesNotExists
	}

	return dialog.ReadBye(req, tx)
}

func buildLocalMedia(kind string, send, recv []*core.Codec) *core.Media {
	hasSend := len(send) > 0
	hasRecv := len(recv) > 0
	if !hasSend && !hasRecv {
		return nil
	}

	codecs := append([]*core.Codec(nil), send...)
	codecs = appendUniqueCodecs(codecs, recv)

	return &core.Media{
		Kind:      kind,
		Direction: mediaDirection(hasSend, hasRecv),
		Codecs:    codecs,
	}
}

func collectSenderCodecs(tracks []*core.Sender, kind string) []*core.Codec {
	codecs := make([]*core.Codec, 0, len(tracks))
	nextDynamic := byte(96)

	for _, track := range tracks {
		if track.Media.Kind != kind {
			continue
		}

		codec := normalizeCodecForSDP(track.Codec, &nextDynamic)
		codecs = appendUniqueCodecs(codecs, []*core.Codec{codec})
	}

	return codecs
}

func collectReceiverCodecs(tracks []*core.Receiver, kind string) []*core.Codec {
	codecs := make([]*core.Codec, 0, len(tracks))
	nextDynamic := byte(96)

	for _, track := range tracks {
		if track.Media.Kind != kind {
			continue
		}

		codec := normalizeCodecForSDP(track.Codec, &nextDynamic)
		codecs = appendUniqueCodecs(codecs, []*core.Codec{codec})
	}

	return codecs
}

func normalizeCodecForSDP(codec *core.Codec, nextDynamic *byte) *core.Codec {
	clone := codec.Clone()
	if clone.ClockRate == 0 && clone.IsVideo() {
		clone.ClockRate = 90000
	}
	if clone.PayloadType == core.PayloadTypeRAW || (clone.IsVideo() && clone.PayloadType < 96) {
		clone.PayloadType = *nextDynamic
		*nextDynamic++
	}
	return clone
}

func appendUniqueCodecs(dst, src []*core.Codec) []*core.Codec {
	for _, codec := range src {
		duplicate := false
		for _, existing := range dst {
			if existing.Match(codec) {
				duplicate = true
				break
			}
		}
		if !duplicate {
			dst = append(dst, codec.Clone())
		}
	}
	return dst
}

func newMediaSession(kind string) (*mediaSession, error) {
	rtpConn, err := net.ListenUDP("udp", &net.UDPAddr{Port: 0})
	if err != nil {
		return nil, err
	}
	return &mediaSession{kind: kind, rtpConn: rtpConn}, nil
}

func cloneSessions(src map[string]*mediaSession) map[string]*mediaSession {
	if len(src) == 0 {
		return nil
	}

	dst := make(map[string]*mediaSession, len(src))
	for kind, session := range src {
		dst[kind] = session
	}
	return dst
}

func closeSessions(sessions map[string]*mediaSession) {
	for _, session := range sessions {
		if session.rtpConn != nil {
			_ = session.rtpConn.Close()
		}
	}
}

func isExpectedClose(err error) bool {
	return err == nil || errors.Is(err, context.Canceled) || errors.Is(err, errSIPClosed)
}

func defaultSIPPort(port int) int {
	if port == 0 {
		return 5060
	}
	return port
}

func supportedAudioCodecs() []*core.Codec {
	return []*core.Codec{
		{Name: core.CodecPCMA, ClockRate: 8000, PayloadType: 8},
		{Name: core.CodecPCMU, ClockRate: 8000, PayloadType: 0},
	}
}

func supportedVideoCodecs() []*core.Codec {
	return []*core.Codec{
		{Name: core.CodecH264, ClockRate: 90000},
		{Name: core.CodecH265, ClockRate: 90000},
	}
}

func (c *Conn) String() string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	parts := make([]string, 0, len(c.sessions))
	for kind, session := range c.sessions {
		codec := ""
		if session.codec != nil {
			codec = session.codec.String()
		}
		parts = append(parts, fmt.Sprintf("%s=%s", kind, codec))
	}

	return fmt.Sprintf("sip medias=%v", parts)
}
