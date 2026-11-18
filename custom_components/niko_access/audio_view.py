"""Live inbound door audio as a playable WAV stream.

Exposes ``/api/niko_access/talk/{serial}/listen.wav`` — a chunked WAV of the
station's inbound talk audio (G.711 u-law transcoded to PCM by ffmpeg). Play it
from a media_player (``media_player.play_media`` with this URL), the dashboard,
or a browser. One shared :class:`TalkBridge` per station backs all listeners.

Talk only runs while a call is active, so this yields audio during a ring/call.
"""

from __future__ import annotations

import asyncio
import logging

from aiohttp import web

from homeassistant.components.ffmpeg import get_ffmpeg_manager
from homeassistant.components.http import HomeAssistantView
from homeassistant.core import HomeAssistant
from homeassistant.helpers import device_registry as dr

from .const import DOMAIN
from .talk_session import TalkBridge

_LOGGER = logging.getLogger(__name__)
_BRIDGES = "talk_bridges"


def _get_bridge(hass: HomeAssistant, serial: str) -> TalkBridge:
    """Reuse or create the one shared TalkBridge for a station."""
    store: dict[str, TalkBridge] = hass.data.setdefault(DOMAIN, {}).setdefault(_BRIDGES, {})
    bridge = store.get(serial)
    if bridge is None:
        coordinator, client = _coordinator_for_serial(hass, serial)
        bridge = TalkBridge(client, serial)
        store[serial] = bridge
    return bridge


def _coordinator_for_serial(hass: HomeAssistant, serial: str):
    """Find the loaded entry/client that owns this serial."""
    device = dr.async_get(hass).async_get_device(identifiers={(DOMAIN, serial)})
    if device:
        for entry_id in device.config_entries:
            entry = hass.config_entries.async_get_entry(entry_id)
            if entry and entry.domain == DOMAIN and entry.runtime_data is not None:
                return entry.runtime_data, entry.runtime_data.client
    raise web.HTTPNotFound(text=f"no loaded Niko Access device {serial}")


class NikoTalkAudioView(HomeAssistantView):
    """Streams live inbound door audio as WAV."""

    url = "/api/niko_access/talk/{serial}/listen.wav"
    name = "api:niko_access:talk_listen"
    requires_auth = True

    async def get(self, request: web.Request, serial: str) -> web.StreamResponse:
        hass: HomeAssistant = request.app["hass"]
        bridge = _get_bridge(hass, serial)
        await bridge.async_start()  # idempotent; opens the talk channel if needed
        queue = bridge.subscribe()

        ffmpeg = get_ffmpeg_manager(hass).binary
        proc = await asyncio.create_subprocess_exec(
            ffmpeg, "-loglevel", "error",
            "-f", "mulaw", "-ar", "8000", "-ac", "1", "-i", "pipe:0",
            "-f", "wav", "pipe:1",
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
        )

        response = web.StreamResponse(headers={"Content-Type": "audio/wav"})
        await response.prepare(request)

        async def _feed() -> None:
            try:
                while True:
                    frame = await queue.get()
                    if frame is None or proc.stdin is None:
                        break
                    proc.stdin.write(frame)
                    await proc.stdin.drain()
            except (ConnectionError, asyncio.CancelledError):
                pass
            finally:
                if proc.stdin and not proc.stdin.is_closing():
                    proc.stdin.close()

        feeder = asyncio.create_task(_feed())
        try:
            assert proc.stdout is not None
            while True:
                chunk = await proc.stdout.read(4096)
                if not chunk:
                    break
                await response.write(chunk)
        except (ConnectionResetError, asyncio.CancelledError):
            pass
        finally:
            feeder.cancel()
            bridge.unsubscribe(queue)
            if proc.returncode is None:
                proc.kill()
            # stop the talk session once nobody is listening
            if not bridge._subscribers:  # noqa: SLF001
                await bridge.async_stop()
                hass.data.get(DOMAIN, {}).get(_BRIDGES, {}).pop(serial, None)
        return response
