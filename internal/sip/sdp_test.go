package sip

import (
	"testing"

	"github.com/AlexxIT/go2rtc/pkg/core"
	"github.com/stretchr/testify/require"
)

func TestBuildOffer(t *testing.T) {
	data, err := BuildOffer("192.168.1.10", 40000, supportedCodecs())
	require.NoError(t, err)
	require.Contains(t, string(data), "m=audio 40000 RTP/AVP 8 0")
	require.Contains(t, string(data), "a=sendrecv")
}

func TestParseAnswer(t *testing.T) {
	answer := []byte("v=0\r\n" +
		"o=- 1 1 IN IP4 192.168.1.20\r\n" +
		"s=go2rtc\r\n" +
		"c=IN IP4 192.168.1.20\r\n" +
		"t=0 0\r\n" +
		"m=audio 50120 RTP/AVP 0 101\r\n" +
		"a=rtpmap:0 PCMU/8000\r\n")

	addr, codec, err := ParseAnswer(answer, []*core.Codec{
		{Name: core.CodecPCMA, ClockRate: 8000, PayloadType: 8},
		{Name: core.CodecPCMU, ClockRate: 8000, PayloadType: 0},
	})
	require.NoError(t, err)
	require.Equal(t, "192.168.1.20:50120", addr.String())
	require.Equal(t, core.CodecPCMU, codec.Name)
	require.Equal(t, uint8(0), codec.PayloadType)
}
