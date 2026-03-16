# SIP

This module adds SIP audio and video calls to go2rtc.

Current scope:

- outbound SIP over UDP
- inbound SIP `INVITE` over UDP
- one-way video from go2rtc to the SIP peer
- `PCMA/8000` and `PCMU/8000`
- `H264/90000` and `H265/90000` passthrough when the source stream exposes them
- bidirectional RTP audio bridge
- `sip:` stream source
- `POST`/`DELETE /api/sip` for on-demand dialing

## Configuration

```yaml
sip:
  listen: ":5060"            # optional, default ":0" for ephemeral local SIP port
  timeout: 30                # optional, INVITE timeout in seconds
  display_name: "Front Door" # optional, SIP From display name for outbound calls
```

`listen` is the local SIP socket go2rtc uses for outbound dialogs and for inbound calls. Leave it as `:0` if you only need outbound dialing. Set it to a fixed reachable address or port, for example `:5060`, if a PBX or softphone will call go2rtc.

## Stream source

You can add a SIP leg directly to a stream:

```yaml
streams:
  doorbell:
    - rtsp://admin:password@192.168.1.100/stream1
    - sip:6001@192.168.1.50:5060
```

When the stream gets a consumer, go2rtc will:

1. start the regular source, for example RTSP
2. place an outbound SIP call
3. bridge stream audio and video to the softphone
4. inject microphone RTP from the softphone back into the stream pipeline

## Inbound Calls

If your phone or PBX should call the doorbell on demand, point the SIP call at the go2rtc listener and use the stream name as the SIP user:

```text
sip:doorbell@go2rtc-host:5060
```

For example, if the stream is named `front-doorbell`, dialing `sip:front-doorbell@go2rtc-host:5060` will attach that call to the `front-doorbell` stream.

This only needs a fixed SIP listener:

```yaml
sip:
  listen: ":5060"
```

When go2rtc answers the call it will:

1. match the called SIP user to an existing stream name
2. answer with `PCMA` or `PCMU`, plus `H264` or `H265` if the stream has video
3. send stream audio and video to the caller
4. inject caller microphone RTP back into the stream pipeline

## Asterisk Example

Register your softphone to Asterisk as usual, then route a local extension to go2rtc:

```ini
; pjsip.conf
[go2rtc]
type=endpoint
transport=transport-udp
context=from-go2rtc
disallow=all
allow=ulaw,alaw,h264
aors=go2rtc

[go2rtc]
type=aor
contact=sip:go2rtc-host:5060
```

```ini
; extensions.conf
[from-internal]
exten => 7001,1,Dial(PJSIP/front-doorbell@go2rtc)
```

In that example:

- your phone dials extension `7001`
- Asterisk sends `INVITE sip:front-doorbell@go2rtc-host:5060`
- go2rtc answers and bridges the call to the `front-doorbell` stream

If you prefer, Asterisk can also route the literal stream name:

```ini
exten => front-doorbell,1,Dial(PJSIP/front-doorbell@go2rtc)
```

## HTTP API

Start a call:

```text
POST /api/sip?src=doorbell&dst=sip:6001@192.168.1.50:5060
```

Stop a call:

```text
DELETE /api/sip?src=doorbell
```

Example:

```bash
curl -X POST "http://localhost:1984/api/sip?src=doorbell&dst=sip:6001@192.168.1.50:5060"
curl -X DELETE "http://localhost:1984/api/sip?src=doorbell"
```

## Home Assistant

```yaml
rest_command:
  doorbell_ring:
    url: "http://127.0.0.1:1984/api/sip"
    method: POST
    params:
      src: doorbell
      dst: "sip:6001@192.168.1.50:5060"

  doorbell_hangup:
    url: "http://127.0.0.1:1984/api/sip"
    method: DELETE
    params:
      src: doorbell
```

## Notes

- The current implementation negotiates `PCMA` or `PCMU` for audio.
- Video is send-only from go2rtc to the SIP peer, with no video backchannel.
- Video is passthrough only. If the source stream does not expose RTP `H264` or `H265`, SIP video will not be negotiated.
- If you route calls through Asterisk, the endpoint that calls go2rtc must allow video codecs too, for example `allow=ulaw,alaw,h264`.
- For best results, make sure the source stream exposes G.711 audio directly or via an existing transcoding source.
- Inbound calling only matches existing go2rtc stream names; there is no SIP registration database inside go2rtc.
