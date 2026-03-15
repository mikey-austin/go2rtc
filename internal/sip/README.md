# SIP

This module adds outbound SIP audio calls to go2rtc.

Current scope:

- outbound SIP over UDP
- audio only
- `PCMA/8000` and `PCMU/8000`
- bidirectional RTP audio bridge
- `sip:` stream source
- `POST`/`DELETE /api/sip` for on-demand dialing

## Configuration

```yaml
sip:
  listen: ":5060"  # optional, default ":0" for ephemeral local SIP port
  timeout: 30      # optional, INVITE timeout in seconds
```

`listen` is the local SIP socket go2rtc uses for outbound dialogs and incoming `BYE` requests from the softphone.

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
3. bridge stream audio to the softphone
4. inject microphone RTP from the softphone back into the stream pipeline

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

- The current implementation negotiates `PCMA` or `PCMU`.
- For best results, make sure the source stream exposes G.711 audio directly or via an existing transcoding source.
- Video is not sent through SIP in this version.
