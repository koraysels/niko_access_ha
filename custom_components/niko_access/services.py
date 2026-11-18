"""Services for Niko Access: speak to the door station (two-way talk uplink).

`niko_access.speak` transcodes an audio source to G.711 u-law (8 kHz mono) with
ffmpeg and pushes it to the station speaker over the reverse-engineered talk
relay. Talk only works while a call is active, so trigger this from a ring
automation (optionally after calling the station's answer).
"""

from __future__ import annotations

import asyncio
import logging

import voluptuous as vol

from homeassistant.components.ffmpeg import get_ffmpeg_manager
from homeassistant.core import HomeAssistant, ServiceCall
from homeassistant.exceptions import HomeAssistantError
from homeassistant.helpers import config_validation as cv, device_registry as dr

from .const import DOMAIN
from .coordinator import NikoAccessCoordinator
from .talk_session import NikoTalkSession, TalkError

_LOGGER = logging.getLogger(__name__)

SERVICE_SPEAK = "speak"
ATTR_MEDIA = "media"

_SPEAK_SCHEMA = vol.Schema(
    {
        vol.Required("device_id"): vol.All(cv.ensure_list, [cv.string]),
        vol.Required(ATTR_MEDIA): cv.string,
    }
)


def _coordinator_for_device(hass: HomeAssistant, device_id: str) -> tuple[NikoAccessCoordinator, str]:
    """Resolve a target device_id to its (coordinator, serial)."""
    device = dr.async_get(hass).async_get(device_id)
    if device is None:
        raise HomeAssistantError(f"Unknown device {device_id}")
    serial = next((i[1] for i in device.identifiers if i[0] == DOMAIN), None)
    if serial is None:
        raise HomeAssistantError(f"{device_id} is not a Niko Access device")
    for entry_id in device.config_entries:
        entry = hass.config_entries.async_get_entry(entry_id)
        if entry and entry.domain == DOMAIN and entry.runtime_data is not None:
            return entry.runtime_data, serial
    raise HomeAssistantError(f"No loaded Niko Access entry for {device_id}")


async def _to_mulaw(hass: HomeAssistant, src: str) -> bytes:
    """Transcode any ffmpeg-readable audio source to raw G.711 u-law 8 kHz mono."""
    ffmpeg = get_ffmpeg_manager(hass).binary
    proc = await asyncio.create_subprocess_exec(
        ffmpeg, "-loglevel", "error", "-i", src,
        "-ar", "8000", "-ac", "1", "-f", "mulaw", "pipe:1",
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
    )
    out, err = await proc.communicate()
    if proc.returncode != 0 or not out:
        raise HomeAssistantError(f"ffmpeg could not read audio {src!r}: {err.decode('utf-8', 'replace')[:200]}")
    return out


async def async_setup_services(hass: HomeAssistant) -> None:
    """Register integration services once."""
    if hass.services.has_service(DOMAIN, SERVICE_SPEAK):
        return

    async def _speak(call: ServiceCall) -> None:
        mulaw = await _to_mulaw(hass, call.data[ATTR_MEDIA])
        bridges = hass.data.get(DOMAIN, {}).get("talk_bridges", {})
        for device_id in call.data["device_id"]:
            coordinator, serial = _coordinator_for_device(hass, device_id)
            bridge = bridges.get(serial)
            if bridge is not None and bridge.active:
                # A listener already holds the talk channel — reuse it (one
                # channel per station; opening a second would conflict).
                await bridge.async_send(mulaw)
                _LOGGER.info("Spoke %d bytes to %s (shared session)", len(mulaw), serial)
                continue
            session = NikoTalkSession(coordinator.client, serial)
            try:
                await session.async_start()
                await session.async_send(mulaw)
            except TalkError as err:
                raise HomeAssistantError(f"Talk to {serial} failed: {err}") from err
            finally:
                await session.async_close()
            _LOGGER.info("Spoke %d bytes of audio to %s", len(mulaw), serial)

    hass.services.async_register(DOMAIN, SERVICE_SPEAK, _speak, schema=_SPEAK_SCHEMA)
