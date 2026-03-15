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

type Conn struct {
	core.Connection

	manager *Manager
	uri     sipmsg.Uri

	mu         sync.RWMutex
	started    bool
	stopped    bool
	clientDlg  *sipgo.DialogClientSession
	serverDlg  *sipgo.DialogServerSession
	onClose    func()
	cancel     context.CancelCauseFunc
	rtpConn    *net.UDPConn
	remoteAddr *net.UDPAddr
	codec      *core.Codec
	readErr    error
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
			Codecs:    supportedCodecs(),
		},
		{
			Kind:      core.KindAudio,
			Direction: core.DirectionRecvonly,
			Codecs:    supportedCodecs(),
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
	rtpConn *net.UDPConn,
	remoteAddr *net.UDPAddr,
	codec *core.Codec,
	answer []byte,
) {
	c.mu.Lock()
	c.serverDlg = dialog
	c.rtpConn = rtpConn
	c.remoteAddr = remoteAddr
	c.codec = codec
	c.SDP = string(answer)
	c.mu.Unlock()
}

func (c *Conn) GetTrack(media *core.Media, codec *core.Codec) (*core.Receiver, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, receiver := range c.Receivers {
		if receiver.Codec.Match(codec) {
			return receiver, nil
		}
	}

	receiver := core.NewReceiver(media, codec.Clone())
	c.Receivers = append(c.Receivers, receiver)
	return receiver, nil
}

func (c *Conn) AddTrack(media *core.Media, codec *core.Codec, track *core.Receiver) error {
	sender := core.NewSender(media, codec.Clone())
	sender.Handler = c.outboundHandler(track.Codec.Clone())
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

	buf := make([]byte, 1500)
	for {
		n, _, err := c.rtpConn.ReadFromUDP(buf)
		if err != nil {
			if errors.Is(err, net.ErrClosed) || context.Cause(ctx) != nil {
				return nil
			}
			c.mu.Lock()
			c.readErr = err
			c.mu.Unlock()
			return err
		}

		packet := &rtp.Packet{}
		if err = packet.Unmarshal(buf[:n]); err != nil {
			continue
		}

		c.Recv += n
		c.dispatchInbound(packet)
	}
}

func (c *Conn) Stop() error {
	c.close(true)
	return nil
}

func (c *Conn) prepare(parent context.Context) error {
	c.mu.RLock()
	ready := c.rtpConn != nil && c.remoteAddr != nil && c.codec != nil
	c.mu.RUnlock()

	if ready {
		return nil
	}

	return c.invite(parent)
}

func (c *Conn) invite(parent context.Context) error {
	localRTP, err := net.ListenUDP("udp", &net.UDPAddr{Port: 0})
	if err != nil {
		return err
	}

	localPort := localRTP.LocalAddr().(*net.UDPAddr).Port
	hostPort := net.JoinHostPort(c.uri.Host, strconv.Itoa(defaultSIPPort(c.uri.Port)))
	contact, localIP, err := c.manager.contactHeader(hostPort)
	if err != nil {
		_ = localRTP.Close()
		return err
	}

	offer, err := BuildOffer(localIP, localPort, supportedCodecs())
	if err != nil {
		_ = localRTP.Close()
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
		_ = localRTP.Close()
		return err
	}

	dialog.OnState(func(state sipmsg.DialogState) {
		if state == sipmsg.DialogStateEnded {
			c.close(false)
		}
	})

	if err = dialog.WaitAnswer(ctx, sipgo.AnswerOptions{}); err != nil {
		_ = localRTP.Close()
		return err
	}

	if err = dialog.Ack(context.Background()); err != nil {
		_ = localRTP.Close()
		return err
	}

	remoteAddr, codec, err := ParseAnswer(dialog.InviteResponse.Body(), supportedCodecs())
	if err != nil {
		_ = localRTP.Close()
		return err
	}

	c.mu.Lock()
	c.clientDlg = dialog
	c.rtpConn = localRTP
	c.remoteAddr = remoteAddr
	c.codec = codec
	c.SDP = string(dialog.InviteResponse.Body())
	c.mu.Unlock()

	return nil
}

func (c *Conn) dispatchInbound(packet *rtp.Packet) {
	c.mu.RLock()
	codec := c.codec
	receivers := append([]*core.Receiver(nil), c.Receivers...)
	c.mu.RUnlock()

	if codec == nil {
		return
	}

	for _, receiver := range receivers {
		clone := *packet
		clone.PayloadType = receiver.Codec.PayloadType

		if receiver.Codec.Name != codec.Name {
			clone.Payload = pcm.Transcode(receiver.Codec, codec)(packet.Payload)
		}

		receiver.WriteRTP(&clone)
	}
}

func (c *Conn) outboundHandler(src *core.Codec) core.HandlerFunc {
	return func(packet *rtp.Packet) {
		c.mu.RLock()
		codec := c.codec
		rtpConn := c.rtpConn
		remoteAddr := c.remoteAddr
		c.mu.RUnlock()

		if codec == nil || rtpConn == nil || remoteAddr == nil {
			return
		}

		clone := *packet
		clone.PayloadType = codec.PayloadType

		if src.Name != codec.Name {
			clone.Payload = pcm.Transcode(codec, src)(packet.Payload)
		}

		data, err := clone.Marshal()
		if err != nil {
			return
		}

		if _, err = rtpConn.WriteToUDP(data, remoteAddr); err == nil {
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
	rtpConn := c.rtpConn
	senders := append([]*core.Sender(nil), c.Senders...)
	receivers := append([]*core.Receiver(nil), c.Receivers...)
	onClose := c.onClose
	c.mu.Unlock()

	if cancel != nil {
		cancel(errors.New("sip closed"))
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

	if rtpConn != nil {
		_ = rtpConn.Close()
	}

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

func defaultSIPPort(port int) int {
	if port == 0 {
		return 5060
	}
	return port
}

func supportedCodecs() []*core.Codec {
	return []*core.Codec{
		{Name: core.CodecPCMA, ClockRate: 8000, PayloadType: 8},
		{Name: core.CodecPCMU, ClockRate: 8000, PayloadType: 0},
	}
}

func (c *Conn) String() string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	codec := ""
	if c.codec != nil {
		codec = c.codec.String()
	}

	return fmt.Sprintf("sip codec=%s remote=%v", codec, c.remoteAddr)
}
