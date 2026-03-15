package sip

import (
	"errors"
	"fmt"
	"net"
	"strconv"

	"github.com/AlexxIT/go2rtc/pkg/core"
	"github.com/pion/sdp/v3"
)

func BuildOffer(localIP string, localPort int, codecs []*core.Codec) ([]byte, error) {
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

	md := &sdp.MediaDescription{
		MediaName: sdp.MediaName{
			Media:   "audio",
			Port:    sdp.RangedPort{Value: localPort},
			Protos:  []string{"RTP", "AVP"},
			Formats: nil,
		},
	}

	md.WithPropertyAttribute(core.DirectionSendRecv)

	for _, codec := range codecs {
		md.WithCodec(codec.PayloadType, codec.Name, codec.ClockRate, uint16(codec.Channels), codec.FmtpLine)
	}

	sd.MediaDescriptions = append(sd.MediaDescriptions, md)
	return sd.Marshal()
}

func ParseAnswer(body []byte, offered []*core.Codec) (*net.UDPAddr, *core.Codec, error) {
	return parseRemoteAudio(body, offered, "answer")
}

func ParseOffer(body []byte, supported []*core.Codec) (*net.UDPAddr, *core.Codec, error) {
	return parseRemoteAudio(body, supported, "offer")
}

func parseRemoteAudio(body []byte, codecs []*core.Codec, kind string) (*net.UDPAddr, *core.Codec, error) {
	sd := &sdp.SessionDescription{}
	if err := sd.Unmarshal(body); err != nil {
		return nil, nil, err
	}

	for _, md := range sd.MediaDescriptions {
		if md.MediaName.Media != "audio" || md.MediaName.Port.Value == 0 {
			continue
		}

		remoteMedia := core.UnmarshalMedia(md)
		for _, offeredCodec := range codecs {
			for _, remoteCodec := range remoteMedia.Codecs {
				if !offeredCodec.Match(remoteCodec) {
					continue
				}

				host := ""
				if md.ConnectionInformation != nil && md.ConnectionInformation.Address != nil {
					host = md.ConnectionInformation.Address.Address
				}
				if host == "" && sd.ConnectionInformation != nil && sd.ConnectionInformation.Address != nil {
					host = sd.ConnectionInformation.Address.Address
				}
				if host == "" {
					return nil, nil, errors.New("sip: missing remote media address")
				}

				addr, err := net.ResolveUDPAddr("udp", net.JoinHostPort(host, strconv.Itoa(md.MediaName.Port.Value)))
				if err != nil {
					return nil, nil, err
				}

				codec := offeredCodec.Clone()
				codec.PayloadType = remoteCodec.PayloadType
				return addr, codec, nil
			}
		}
	}

	return nil, nil, fmt.Errorf("sip: no matching codec in %s", kind)
}
