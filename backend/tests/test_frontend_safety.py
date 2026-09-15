"""Static regression checks on the frontend pages.

XSS: rider-controlled values (signup names, place names, restaurant/item
names) reach the driver page through Socket.IO. The pages must render them
as text, so HTML-parsing sinks are banned outright. There is no JS test
runner in this project, so these are source checks, not browser tests.
"""
import os
import re

import pytest

FRONTEND = os.path.join(os.path.dirname(__file__), "..", "..", "frontend")
PAGES = ["driver.html", "new_map.html", "login.html", "admin.html"]
HTML_SINKS = ["innerHTML", "outerHTML", "insertAdjacentHTML", "document.write", "DOMParser", "createContextualFragment"]


def _read(page):
    with open(os.path.join(FRONTEND, page), encoding="utf-8") as f:
        return f.read()


@pytest.mark.parametrize("page", PAGES)
def test_pages_never_parse_strings_as_html(page):
    source = _read(page)
    found = [sink for sink in HTML_SINKS if sink in source]
    assert not found, f"{page} uses HTML-parsing sinks {found}; build DOM nodes with textContent instead"


def test_leaflet_markers_do_not_receive_dynamic_html():
    for page in ("driver.html", "new_map.html"):
        for match in re.finditer(r"L\.divIcon\(\{\s*html:\s*([^,}]+)", _read(page)):
            assert re.fullmatch(r"'[^'$]*'", match.group(1).strip()), f"{page}: divIcon html must be a constant"
        assert "bindPopup" not in _read(page) and "bindTooltip" not in _read(page)


def test_driver_heartbeat_has_single_timer_and_stops():
    source = _read("driver.html")
    assert re.search(r"function startHeartbeat\(\)\s*\{\s*stopHeartbeat\(\);", source), \
        "startHeartbeat must clear any existing timer before creating one"
    assert "socket.on('disconnect', stopHeartbeat)" in source
    assert re.search(r"function logout\(\)\s*\{\s*stopHeartbeat\(\);", source)
    assert "socket.emit('driver_heartbeat'" in source
    assert "heartbeat_interval_seconds" in source
