package sip

import (
	"errors"
	"net"
	"strconv"

	"github.com/AlexxIT/go2rtc/pkg/core"
	"github.com/pion/sdp/v3"
)

type NegotiatedMedia struct {
	Kind      string
	Direction string
	Codec     *core.Codec
	Addr      *net.UDPAddr
}

func BuildOffer(localIP string, ports map[string]int, medias []*core.Media) ([]byte, error) {
	addressType := "IP4"
	if net.ParseIP(localIP).To4() == nil {
		addressType = "IP6"
	}

	sd := &sdp.SessionDescription{
		Origin: sdp.Origin{
			Username:       "-",
			SessionID:      1,
			SessionVersion: 1,
			NetworkType:    "IN",
			AddressType:    addressType,
			UnicastAddress: localIP,
		},
		SessionName: sdp.SessionName("go2rtc"),
		ConnectionInformation: &sdp.ConnectionInformation{
			NetworkType: "IN",
			AddressType: addressType,
			Address: &sdp.Address{
				Address: localIP,
			},
		},
		TimeDescriptions: []sdp.TimeDescription{{Timing: sdp.Timing{}}},
	}

	for _, media := range medias {
		port := ports[media.Kind]
		if port == 0 || len(media.Codecs) == 0 {
			continue
		}

		md := &sdp.MediaDescription{
			MediaName: sdp.MediaName{
				Media:   media.Kind,
				Port:    sdp.RangedPort{Value: port},
				Protos:  []string{"RTP", "AVP"},
				Formats: nil,
			},
		}

		switch media.Direction {
		case core.DirectionSendonly:
			md.WithPropertyAttribute(core.DirectionSendonly)
		case core.DirectionRecvonly:
			md.WithPropertyAttribute(core.DirectionRecvonly)
		default:
			md.WithPropertyAttribute(core.DirectionSendRecv)
		}

		for _, codec := range media.Codecs {
			md.WithCodec(codec.PayloadType, codec.Name, codec.ClockRate, uint16(codec.Channels), codec.FmtpLine)
		}

		sd.MediaDescriptions = append(sd.MediaDescriptions, md)
	}

	return sd.Marshal()
}

func ParseAnswer(body []byte, offered []*core.Media) (map[string]*NegotiatedMedia, error) {
	return parseNegotiated(body, offered)
}

func ParseOffer(body []byte, offered []*core.Media) (map[string]*NegotiatedMedia, error) {
	return parseNegotiated(body, offered)
}

func AnswerOffer(body []byte, local []*core.Media) ([]*core.Media, map[string]*NegotiatedMedia, error) {
	sd := &sdp.SessionDescription{}
	if err := sd.Unmarshal(body); err != nil {
		return nil, nil, err
	}

	var answer []*core.Media
	remote := make(map[string]*NegotiatedMedia, len(local))

	for _, md := range sd.MediaDescriptions {
		if md.MediaName.Port.Value == 0 {
			continue
		}

		offerMedia := core.UnmarshalMedia(md)
		if offerMedia.Direction == "" {
			offerMedia.Direction = core.DirectionSendRecv
		}

		host, err := mediaHost(sd, md)
		if err != nil {
			return nil, nil, err
		}

		for _, localMedia := range local {
			if localMedia.Kind != offerMedia.Kind {
				continue
			}

			send := mediaCanSend(localMedia.Direction) && mediaCanRecv(offerMedia.Direction)
			recv := mediaCanRecv(localMedia.Direction) && mediaCanSend(offerMedia.Direction)
			if !send && !recv {
				continue
			}

			localCodec, remoteCodec := matchCodecs(localMedia.Codecs, offerMedia.Codecs)
			if localCodec == nil {
				continue
			}

			addr, err := net.ResolveUDPAddr("udp", net.JoinHostPort(host, strconv.Itoa(md.MediaName.Port.Value)))
			if err != nil {
				return nil, nil, err
			}

			codec := localCodec.Clone()
			codec.PayloadType = remoteCodec.PayloadType

			direction := mediaDirection(send, recv)
			answer = append(answer, &core.Media{
				Kind:      localMedia.Kind,
				Direction: direction,
				Codecs:    []*core.Codec{codec},
			})
			remote[localMedia.Kind] = &NegotiatedMedia{
				Kind:      localMedia.Kind,
				Direction: direction,
				Codec:     codec.Clone(),
				Addr:      addr,
			}
			break
		}
	}

	if len(answer) == 0 {
		return nil, nil, errors.New("sip: no matching codec in offer")
	}

	return answer, remote, nil
}

func parseNegotiated(body []byte, offered []*core.Media) (map[string]*NegotiatedMedia, error) {
	sd := &sdp.SessionDescription{}
	if err := sd.Unmarshal(body); err != nil {
		return nil, err
	}

	remote := make(map[string]*NegotiatedMedia, len(offered))

	for _, md := range sd.MediaDescriptions {
		if md.MediaName.Port.Value == 0 {
			continue
		}

		remoteMedia := core.UnmarshalMedia(md)
		if remoteMedia.Direction == "" {
			remoteMedia.Direction = core.DirectionSendRecv
		}

		host, err := mediaHost(sd, md)
		if err != nil {
			return nil, err
		}

		for _, offeredMedia := range offered {
			if offeredMedia.Kind != remoteMedia.Kind {
				continue
			}

			send := mediaCanSend(offeredMedia.Direction) && mediaCanRecv(remoteMedia.Direction)
			recv := mediaCanRecv(offeredMedia.Direction) && mediaCanSend(remoteMedia.Direction)
			if !send && !recv {
				continue
			}

			offeredCodec, remoteCodec := matchCodecs(offeredMedia.Codecs, remoteMedia.Codecs)
			if offeredCodec == nil {
				continue
			}

			addr, err := net.ResolveUDPAddr("udp", net.JoinHostPort(host, strconv.Itoa(md.MediaName.Port.Value)))
			if err != nil {
				return nil, err
			}

			codec := offeredCodec.Clone()
			codec.PayloadType = remoteCodec.PayloadType
			remote[offeredMedia.Kind] = &NegotiatedMedia{
				Kind:      offeredMedia.Kind,
				Direction: mediaDirection(send, recv),
				Codec:     codec,
				Addr:      addr,
			}
			break
		}
	}

	if len(remote) == 0 {
		return nil, errors.New("sip: no matching codec in answer")
	}

	return remote, nil
}

func mediaHost(sd *sdp.SessionDescription, md *sdp.MediaDescription) (string, error) {
	if md.ConnectionInformation != nil && md.ConnectionInformation.Address != nil && md.ConnectionInformation.Address.Address != "" {
		return md.ConnectionInformation.Address.Address, nil
	}
	if sd.ConnectionInformation != nil && sd.ConnectionInformation.Address != nil && sd.ConnectionInformation.Address.Address != "" {
		return sd.ConnectionInformation.Address.Address, nil
	}
	return "", errors.New("sip: missing remote media address")
}

func matchCodecs(local, remote []*core.Codec) (*core.Codec, *core.Codec) {
	for _, localCodec := range local {
		for _, remoteCodec := range remote {
			if localCodec.Match(remoteCodec) {
				return localCodec, remoteCodec
			}
		}
	}
	return nil, nil
}

func mediaCanSend(direction string) bool {
	return direction == "" || direction == core.DirectionSendonly || direction == core.DirectionSendRecv
}

func mediaCanRecv(direction string) bool {
	return direction == "" || direction == core.DirectionRecvonly || direction == core.DirectionSendRecv
}

func mediaDirection(send, recv bool) string {
	switch {
	case send && recv:
		return core.DirectionSendRecv
	case send:
		return core.DirectionSendonly
	case recv:
		return core.DirectionRecvonly
	default:
		return ""
	}
}
