package sip

import (
	"context"
	"errors"
	"fmt"
	"net"
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
	dialog     *sipgo.DialogClientSession
	cancel     context.CancelCauseFunc
	rtpConn    *net.UDPConn
	remoteAddr *net.UDPAddr
	codec      *core.Codec
	readErr    error
}

func NewConn(manager *Manager, rawURL string, uri sipmsg.Uri) *Conn {
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
			RemoteAddr: uri.HostPort(),
			Source:     rawURL,
			URL:        rawURL,
			Medias:     medias,
		},
		manager: manager,
		uri:     uri,
	}
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

	if err := c.invite(ctx); err != nil {
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

func (c *Conn) invite(parent context.Context) error {
	localRTP, err := net.ListenUDP("udp", &net.UDPAddr{Port: 0})
	if err != nil {
		return err
	}

	localPort := localRTP.LocalAddr().(*net.UDPAddr).Port
	contact, localIP, err := c.manager.contactHeader(c.uri)
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
	c.dialog = dialog
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
	dialog := c.dialog
	rtpConn := c.rtpConn
	senders := append([]*core.Sender(nil), c.Senders...)
	receivers := append([]*core.Receiver(nil), c.Receivers...)
	c.mu.Unlock()

	if cancel != nil {
		cancel(errors.New("sip closed"))
	}

	if sendBye && dialog != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		_ = dialog.Bye(ctx)
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
