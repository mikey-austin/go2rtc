package sip

import (
	"testing"

	"github.com/AlexxIT/go2rtc/pkg/core"
	"github.com/stretchr/testify/require"
)

func TestBuildOffer(t *testing.T) {
	medias := []*core.Media{
		{Kind: core.KindAudio, Direction: core.DirectionSendRecv, Codecs: []*core.Codec{{Name: core.CodecPCMU, ClockRate: 8000, PayloadType: 0}}},
		{Kind: core.KindVideo, Direction: core.DirectionSendonly, Codecs: []*core.Codec{{Name: core.CodecH264, ClockRate: 90000, PayloadType: 96}}},
	}

	data, err := BuildOffer("192.168.1.10", map[string]int{
		core.KindAudio: 40000,
		core.KindVideo: 40002,
	}, medias)
	require.NoError(t, err)
	require.Contains(t, string(data), "m=audio 40000 RTP/AVP 0")
	require.Contains(t, string(data), "m=video 40002 RTP/AVP 96")
	require.Contains(t, string(data), "a=sendrecv")
	require.Contains(t, string(data), "a=sendonly")
}

func TestParseAnswer(t *testing.T) {
	answer := []byte("v=0\r\n" +
		"o=- 1 1 IN IP4 192.168.1.20\r\n" +
		"s=go2rtc\r\n" +
		"c=IN IP4 192.168.1.20\r\n" +
		"t=0 0\r\n" +
		"m=audio 50120 RTP/AVP 0\r\n" +
		"a=sendrecv\r\n" +
		"a=rtpmap:0 PCMU/8000\r\n" +
		"m=video 50122 RTP/AVP 96\r\n" +
		"a=recvonly\r\n" +
		"a=rtpmap:96 H264/90000\r\n")

	remote, err := ParseAnswer(answer, []*core.Media{
		{Kind: core.KindAudio, Direction: core.DirectionSendRecv, Codecs: []*core.Codec{{Name: core.CodecPCMU, ClockRate: 8000, PayloadType: 0}}},
		{Kind: core.KindVideo, Direction: core.DirectionSendonly, Codecs: []*core.Codec{{Name: core.CodecH264, ClockRate: 90000, PayloadType: 96}}},
	})
	require.NoError(t, err)
	require.Equal(t, "192.168.1.20:50120", remote[core.KindAudio].Addr.String())
	require.Equal(t, core.CodecPCMU, remote[core.KindAudio].Codec.Name)
	require.Equal(t, core.DirectionSendRecv, remote[core.KindAudio].Direction)
	require.Equal(t, "192.168.1.20:50122", remote[core.KindVideo].Addr.String())
	require.Equal(t, core.CodecH264, remote[core.KindVideo].Codec.Name)
	require.Equal(t, core.DirectionSendonly, remote[core.KindVideo].Direction)
}
