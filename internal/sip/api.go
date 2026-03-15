package sip

import (
	"context"
	"errors"
	"net"
	"net/http"

	"github.com/AlexxIT/go2rtc/internal/api"
	"github.com/AlexxIT/go2rtc/internal/streams"
)

type call struct {
	stream *streams.Stream
	conn   *Conn
}

func handleAPI(w http.ResponseWriter, r *http.Request) {
	src := r.URL.Query().Get("src")
	if src == "" {
		http.Error(w, "missing src", http.StatusBadRequest)
		return
	}

	switch r.Method {
	case http.MethodPost:
		dst := r.URL.Query().Get("dst")
		if dst == "" {
			http.Error(w, "missing dst", http.StatusBadRequest)
			return
		}

		stream := streams.Get(src)
		if stream == nil {
			http.Error(w, api.StreamNotFound, http.StatusNotFound)
			return
		}

		if active, ok := calls.Load(src); ok {
			stopCall(src, active.(*call))
		}

		conn, err := manager.newConn(dst)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		stream.AddProducer(conn)
		if err = stream.AddConsumer(conn); err != nil {
			stream.RemoveProducer(conn)
			_ = conn.Stop()
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		current := &call{stream: stream, conn: conn}
		calls.Store(src, current)

		go func() {
			err := conn.Start()
			stopCall(src, current)

			if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, net.ErrClosed) {
				return
			}

			log.Warn().Err(err).Str("src", src).Str("dst", dst).Msg("[sip] call ended")
		}()

		w.WriteHeader(http.StatusCreated)

	case http.MethodDelete:
		active, ok := calls.Load(src)
		if ok {
			stopCall(src, active.(*call))
		}
		w.WriteHeader(http.StatusNoContent)

	default:
		http.Error(w, "", http.StatusMethodNotAllowed)
	}
}

func stopCall(key string, current *call) {
	calls.Delete(key)

	current.stream.RemoveProducer(current.conn)
	current.stream.RemoveConsumer(current.conn)
}
