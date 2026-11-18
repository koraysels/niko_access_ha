"""Two-way talk session glue: wire the vendored CPD7 NikoTalkClient into HA.

The hard protocol work already lives in ``cpd7/talk.py`` (NikoTalkClient — the
G.711 u-law duplex over the cloud TTS relay). This module supplies the two
runtime inputs that client needs and runs its blocking sockets off the event
loop:

  * the TTS relay address (``ttsAddr``/``ttsPort``) from
    ``GET /v3/configurations/system/info``;
  * the ``ssn`` session token, which the API's ``get_ticket_info`` documents as
    the value used in the ``tts://...talk://`` URL.

Talk only works while a call is active (the station must be in ringing/in-call),
so callers should start a session from a ring event or after ``answer_call``.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from .api import NikoAccessClient
from .cpd7.talk import NikoTalkClient

_LOGGER = logging.getLogger(__name__)

# systemConfigInfo keys that have held the TTS relay on Hik-Connect/EZVIZ EU.
_RELAY_HOST_KEYS = ("ttsAddr", "ttsServerAddr", "ttsServer", "ttsIp")
_RELAY_PORT_KEYS = ("ttsPort", "ttsServerPort")
_DEFAULT_RELAY_PORT = 9664


def _resolve_relay(sysinfo: dict[str, Any]) -> tuple[str, int]:
    """Pull the TTS relay host:port out of systemConfigInfo (naming varies)."""
    host = next((str(sysinfo[k]) for k in _RELAY_HOST_KEYS if sysinfo.get(k)), None)
    port = next((int(sysinfo[k]) for k in _RELAY_PORT_KEYS if sysinfo.get(k)), _DEFAULT_RELAY_PORT)
    if not host:
        # Log the whole blob once so the real field name is easy to spot on the
        # first live call, then fail clearly rather than guessing.
        _LOGGER.error("No TTS relay field in systemConfigInfo; keys=%s", list(sysinfo))
        raise TalkError("TTS relay address not found in systemConfigInfo")
    _LOGGER.debug("Resolved TTS relay %s:%s", host, port)
    return host, port


class TalkError(RuntimeError):
    """Talk setup/transport failed."""


class NikoTalkSession:
    """One two-way talk session for one door station, usable from async HA code.

    Wraps the blocking ``NikoTalkClient`` with executor calls. Audio is exchanged
    as raw G.711 u-law, 8 kHz mono (what the relay speaks); transcoding to/from
    that is the caller's job (ffmpeg).
    """

    def __init__(self, client: NikoAccessClient, serial: str) -> None:
        self._client = client
        self._serial = serial
        self._talk: NikoTalkClient | None = None

    async def async_start(self) -> str:
        """Resolve relay + ssn and open the talk channel. Returns the codec line."""
        sysinfo = await self._client.get_system_info()
        relay_host, relay_port = _resolve_relay(sysinfo)
        ticket = (await self._client.get_ticket_info(self._serial)).get("ticket")
        if not ticket:
            raise TalkError("no stream ticket (ssn) returned for talk")
        talk = NikoTalkClient(relay_host, relay_port, self._serial, ticket)
        try:
            codec = await asyncio.get_running_loop().run_in_executor(None, talk.start)
        except Exception as err:  # noqa: BLE001 - surface any transport failure
            raise TalkError(f"talk invite failed: {err}") from err
        self._talk = talk
        _LOGGER.info("Talk session open for %s (%s)", self._serial, codec.strip())
        return codec

    async def async_send(self, mulaw: bytes) -> None:
        """Send G.711 u-law mic audio to the door speaker."""
        if not self._talk:
            raise TalkError("talk session not started")
        await asyncio.get_running_loop().run_in_executor(None, self._talk.send_audio, mulaw)

    async def async_read(self) -> bytes | None:
        """Read one inbound G.711 u-law frame from the door (or None on timeout)."""
        if not self._talk:
            raise TalkError("talk session not started")
        return await asyncio.get_running_loop().run_in_executor(None, self._talk.read_audio)

    async def async_close(self) -> None:
        if self._talk:
            talk, self._talk = self._talk, None
            await asyncio.get_running_loop().run_in_executor(None, talk.close)


class TalkBridge:
    """Full-duplex talk: one open session, a background reader that fans the
    door's inbound G.711 u-law out to subscribers, and an uplink to the speaker.

    Inbound audio is raw G.711 u-law (8 kHz mono); subscribers transcode it
    (e.g. ffmpeg -> WAV) for playback.
    """

    def __init__(self, client: NikoAccessClient, serial: str) -> None:
        self._session = NikoTalkSession(client, serial)
        self._subscribers: set[asyncio.Queue[bytes | None]] = set()
        self._reader: asyncio.Task | None = None
        self._lock = asyncio.Lock()

    @property
    def active(self) -> bool:
        return self._reader is not None and not self._reader.done()

    async def async_start(self) -> str:
        async with self._lock:
            if self.active:
                return "already running"
            codec = await self._session.async_start()
            self._reader = asyncio.create_task(self._read_loop())
            return codec

    async def _read_loop(self) -> None:
        try:
            while True:
                frame = await self._session.async_read()
                if frame is None:  # timeout — keep waiting while the call lasts
                    continue
                if not frame:  # non-audio control frame
                    continue
                for q in list(self._subscribers):
                    q.put_nowait(frame)
        except Exception as err:  # noqa: BLE001
            _LOGGER.debug("talk read loop ended: %s", err)
        finally:
            for q in list(self._subscribers):
                q.put_nowait(None)

    def subscribe(self) -> asyncio.Queue[bytes | None]:
        q: asyncio.Queue[bytes | None] = asyncio.Queue(maxsize=256)
        self._subscribers.add(q)
        return q

    def unsubscribe(self, q: asyncio.Queue[bytes | None]) -> None:
        self._subscribers.discard(q)

    async def async_send(self, mulaw: bytes) -> None:
        await self._session.async_send(mulaw)

    async def async_stop(self) -> None:
        async with self._lock:
            if self._reader:
                self._reader.cancel()
                self._reader = None
            await self._session.async_close()
