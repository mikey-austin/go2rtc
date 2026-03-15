package sip

import (
	"errors"
	"fmt"
	"net"
	"strconv"
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
	Listen  string `yaml:"listen"`
	Timeout int    `yaml:"timeout"`
}

func Init() {
	var cfg struct {
		Mod Config `yaml:"sip"`
	}

	cfg.Mod.Timeout = 30

	app.LoadConfig(&cfg)

	log = app.GetLogger("sip")
	manager = &Manager{
		listen:  cfg.Mod.Listen,
		timeout: time.Duration(cfg.Mod.Timeout) * time.Second,
	}

	streams.HandleFunc("sip", Dial)
	api.HandleFunc("api/sip", handleAPI)
}

var (
	log     zerolog.Logger
	manager *Manager
	calls   sync.Map
)

type Manager struct {
	listen  string
	timeout time.Duration

	mu         sync.Mutex
	ua         *sipgo.UserAgent
	srv        *sipgo.Server
	dialogs    *sipgo.DialogClientCache
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

	srv.OnBye(func(req *sipmsg.Request, tx sipmsg.ServerTransaction) {
		if err := m.dialogs.ReadBye(req, tx); err != nil {
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

	return NewConn(m, rawURL, uri), nil
}

func (m *Manager) contactHeader(dst sipmsg.Uri) (*sipmsg.ContactHeader, string, error) {
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
		port := dst.Port
		if port == 0 {
			port = 5060
		}

		conn, err := net.Dial("udp", net.JoinHostPort(dst.Host, strconv.Itoa(port)))
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

func Dial(rawURL string) (core.Producer, error) {
	return manager.newConn(rawURL)
}
