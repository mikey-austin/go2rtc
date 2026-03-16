package sip

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/AlexxIT/go2rtc/internal/api"
	"github.com/AlexxIT/go2rtc/internal/app"
	"github.com/AlexxIT/go2rtc/internal/streams"
	"github.com/AlexxIT/go2rtc/pkg/core"
	"github.com/emiago/sipgo"
	sipmsg "github.com/emiago/sipgo/sip"
	"github.com/rs/zerolog"
)

type Config struct {
	Listen      string `yaml:"listen"`
	Timeout     int    `yaml:"timeout"`
	DisplayName string `yaml:"display_name"`
}

func Init() {
	var cfg struct {
		Mod Config `yaml:"sip"`
	}

	cfg.Mod.Timeout = 30

	app.LoadConfig(&cfg)

	log = app.GetLogger("sip")
	manager = &Manager{
		listen:      cfg.Mod.Listen,
		timeout:     time.Duration(cfg.Mod.Timeout) * time.Second,
		displayName: cfg.Mod.DisplayName,
	}

	streams.HandleFunc("sip", Dial)
	api.HandleFunc("api/sip", handleAPI)

	// Start the SIP listener eagerly if a listen address is configured,
	// so inbound INVITEs can be received without waiting for first use.
	if cfg.Mod.Listen != "" {
		if err := manager.ensureServer(); err != nil {
			log.Error().Err(err).Msg("[sip] failed to start listener")
		}
	}
}

var (
	log     zerolog.Logger
	manager *Manager
	calls   sync.Map
)

type Manager struct {
	listen      string
	timeout     time.Duration
	displayName string

	mu         sync.Mutex
	ua         *sipgo.UserAgent
	srv        *sipgo.Server
	client     *sipgo.Client
	dialogs    *sipgo.DialogClientCache
	inbound    sync.Map
	listener   net.PacketConn
	listenAddr *net.UDPAddr
}

func (m *Manager) ensureServer() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.dialogs != nil {
		return nil
	}

	ua, err := sipgo.NewUA(sipgo.WithUserAgent(app.UserAgent))
	if err != nil {
		return err
	}

	srv, err := sipgo.NewServer(ua)
	if err != nil {
		_ = ua.Close()
		return err
	}

	srv.OnInvite(func(req *sipmsg.Request, tx sipmsg.ServerTransaction) {
		m.handleInvite(req, tx)
	})

	srv.OnAck(func(req *sipmsg.Request, tx sipmsg.ServerTransaction) {
		if err := m.handleAck(req, tx); err != nil {
			log.Debug().Err(err).Str("source", req.Source()).Msg("[sip] ack")
		}
	})

	srv.OnBye(func(req *sipmsg.Request, tx sipmsg.ServerTransaction) {
		if err := m.handleBye(req, tx); err != nil {
			res := sipmsg.NewResponseFromRequest(req, 481, "Call/Transaction Does Not Exist", nil)
			_ = tx.Respond(res)
		}
	})

	address := m.listen
	if address == "" {
		address = ":0"
	}

	ln, err := net.ListenPacket("udp", address)
	if err != nil {
		_ = ua.Close()
		return err
	}

	addr, ok := ln.LocalAddr().(*net.UDPAddr)
	if !ok {
		_ = ln.Close()
		_ = ua.Close()
		return errors.New("sip: unexpected listen address")
	}

	client, err := sipgo.NewClient(ua)
	if err != nil {
		_ = ln.Close()
		_ = ua.Close()
		return err
	}

	m.ua = ua
	m.srv = srv
	m.client = client
	m.dialogs = sipgo.NewDialogClientCache(client, sipmsg.ContactHeader{})
	m.listener = ln
	m.listenAddr = addr

	go func() {
		if err := srv.ServeUDP(ln); err != nil {
			log.Warn().Err(err).Msg("[sip] serve")
		}
	}()

	log.Info().Stringer("addr", addr).Msg("[sip] listen udp")

	return nil
}

func (m *Manager) handleInvite(req *sipmsg.Request, tx sipmsg.ServerTransaction) {
	streamName := requestStreamName(req)
	if streamName == "" {
		res := sipmsg.NewResponseFromRequest(req, 400, "Bad Request", nil)
		_ = tx.Respond(res)
		return
	}

	stream := streams.Get(streamName)
	if stream == nil {
		res := sipmsg.NewResponseFromRequest(req, 404, "Not Found", nil)
		_ = tx.Respond(res)
		return
	}

	contact, localIP, err := m.contactHeader(req.Source())
	if err != nil {
		log.Warn().Err(err).Str("stream", streamName).Msg("[sip] contact")
		res := sipmsg.NewResponseFromRequest(req, 500, "Server Error", nil)
		_ = tx.Respond(res)
		return
	}

	dialogUA := &sipgo.DialogUA{
		Client:     m.client,
		ContactHDR: *contact,
	}
	dialog, err := dialogUA.ReadInvite(req, tx)
	if err != nil {
		res := sipmsg.NewResponseFromRequest(req, 400, "Bad Request", nil)
		_ = tx.Respond(res)
		return
	}

	conn := NewInboundConn(m, req, streamName)

	stream.AddProducer(conn)
	if err = stream.AddConsumer(conn); err != nil {
		detachStreamConn(stream, conn)
		_ = conn.Stop()
		res := sipmsg.NewResponseFromRequest(req, 500, "Server Error", nil)
		_ = tx.Respond(res)
		return
	}

	localMedias, sessions, ports, err := conn.localMedias()
	if err != nil {
		detachStreamConn(stream, conn)
		_ = conn.Stop()
		res := sipmsg.NewResponseFromRequest(req, 488, "Not Acceptable Here", nil)
		_ = tx.Respond(res)
		return
	}

	answerMedias, remote, err := AnswerOffer(req.Body(), localMedias)
	if err != nil {
		closeSessions(sessions)
		detachStreamConn(stream, conn)
		_ = conn.Stop()
		log.Debug().Err(err).Str("stream", streamName).Msg("[sip] offer")
		res := sipmsg.NewResponseFromRequest(req, 488, "Not Acceptable Here", nil)
		_ = tx.Respond(res)
		return
	}

	answer, err := BuildOffer(localIP, ports, answerMedias)
	if err != nil {
		closeSessions(sessions)
		detachStreamConn(stream, conn)
		_ = conn.Stop()
		res := sipmsg.NewResponseFromRequest(req, 500, "Server Error", nil)
		_ = tx.Respond(res)
		return
	}

	conn.attachInbound(dialog, sessions, remote, answer)
	conn.onClose = func() {
		m.inbound.Delete(dialog.ID)
		detachStreamConn(stream, conn)
	}

	dialog.OnState(func(state sipmsg.DialogState) {
		if state == sipmsg.DialogStateEnded {
			conn.close(false)
		}
	})

	m.inbound.Store(dialog.ID, conn)
	go m.runCall(conn, streamName, req.Source())

	if err = dialog.Respond(200, "OK", answer, sipmsg.NewHeader("Content-Type", "application/sdp"), contact); err != nil {
		log.Debug().Err(err).Str("stream", streamName).Msg("[sip] answer")
		_ = conn.Stop()
		return
	}
}

func (m *Manager) handleAck(req *sipmsg.Request, tx sipmsg.ServerTransaction) error {
	conn, err := m.inboundConn(req)
	if err != nil {
		return err
	}
	return conn.readAck(req, tx)
}

func (m *Manager) handleBye(req *sipmsg.Request, tx sipmsg.ServerTransaction) error {
	conn, err := m.inboundConn(req)
	if err == nil {
		return conn.readBye(req, tx)
	}
	return m.dialogs.ReadBye(req, tx)
}

func (m *Manager) inboundConn(req *sipmsg.Request) (*Conn, error) {
	id, err := sipmsg.DialogIDFromRequestUAS(req)
	if err != nil {
		return nil, err
	}

	if value, ok := m.inbound.Load(id); ok {
		return value.(*Conn), nil
	}

	return nil, sipgo.ErrDialogDoesNotExists
}

func (m *Manager) newConn(rawURL string) (*Conn, error) {
	if err := m.ensureServer(); err != nil {
		return nil, err
	}

	uri := sipmsg.Uri{}
	if err := sipmsg.ParseUri(rawURL, &uri); err != nil {
		return nil, err
	}

	if transport, _ := uri.UriParams.Get("transport"); transport != "" && !strings.EqualFold(transport, "udp") {
		return nil, fmt.Errorf("sip: unsupported transport: %s", transport)
	}

	conn := NewConn(m, rawURL, uri)
	conn.displayName = m.displayName
	return conn, nil
}

func (m *Manager) contactHeader(target string) (*sipmsg.ContactHeader, string, error) {
	if err := m.ensureServer(); err != nil {
		return nil, "", err
	}

	host := ""
	if listenHost, _, err := net.SplitHostPort(m.listenAddr.String()); err == nil {
		if listenHost != "" && listenHost != "0.0.0.0" && listenHost != "::" {
			host = listenHost
		}
	}

	if host == "" {
		conn, err := net.Dial("udp", target)
		if err != nil {
			return nil, "", err
		}

		if addr, ok := conn.LocalAddr().(*net.UDPAddr); ok {
			host = addr.IP.String()
		}
		_ = conn.Close()
	}

	if host == "" {
		return nil, "", errors.New("sip: can't resolve local contact host")
	}

	return &sipmsg.ContactHeader{
		Address: sipmsg.Uri{
			Scheme: "sip",
			User:   "go2rtc",
			Host:   host,
			Port:   m.listenAddr.Port,
		},
	}, host, nil
}

func (m *Manager) runCall(conn *Conn, src, dst string) {
	err := conn.Start()
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, net.ErrClosed) {
		return
	}

	log.Warn().Err(err).Str("src", src).Str("dst", dst).Msg("[sip] call ended")
}

func requestStreamName(req *sipmsg.Request) string {
	if req.Recipient.User != "" {
		return req.Recipient.User
	}

	if to := req.To(); to != nil && to.Address.User != "" {
		return to.Address.User
	}

	return ""
}

func Dial(rawURL string) (core.Producer, error) {
	return manager.newConn(rawURL)
}
