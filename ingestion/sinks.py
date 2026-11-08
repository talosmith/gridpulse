"""Where raw envelopes land: a local folder or a Databricks Unity Catalog Volume.

Layout (identical in both sinks, so Auto Loader sees the same thing):
    <root>/<source>/<dataset>/ingest_date=YYYY-MM-DD/<dataset>_<start>_<end>_<fetched>.json
"""
from __future__ import annotations

import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Protocol

from .fetch import Envelope


def relative_path(envelope: Envelope) -> str:
    stamp = envelope.fetched_at.strftime("%Y%m%dT%H%M%SZ")
    return (
        f"{envelope.source}/{envelope.dataset}/"
        f"ingest_date={envelope.fetched_at.date().isoformat()}/"
        f"{envelope.dataset}_{envelope.window_start}_{envelope.window_end}_{stamp}.json"
    )


class Sink(Protocol):
    def write(self, envelope: Envelope) -> str: ...


class LocalSink:
    def __init__(self, root: str | Path):
        self.root = Path(root)

    def write(self, envelope: Envelope) -> str:
        target = self.root / relative_path(envelope)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(envelope.to_json(), encoding="utf-8")
        return str(target)


class DatabricksVolumeSink:
    """Uploads through the Databricks Files API (works on Free Edition).

    volume_root example: /Volumes/gridpulse/raw/landing
    """

    def __init__(self, host: str, token: str, volume_root: str):
        if not volume_root.startswith("/Volumes/"):
            raise ValueError("volume_root must look like /Volumes/<catalog>/<schema>/<volume>")
        self.host = host.rstrip("/")
        if not self.host.startswith("https://"):
            self.host = f"https://{self.host}"
        self.token = token
        self.volume_root = volume_root.rstrip("/")

    def write(self, envelope: Envelope) -> str:
        path = f"{self.volume_root}/{relative_path(envelope)}"
        self._request("PUT", f"/api/2.0/fs/directories{_quote(path.rsplit('/', 1)[0])}")
        self._request(
            "PUT",
            f"/api/2.0/fs/files{_quote(path)}?overwrite=true",
            data=envelope.to_json().encode("utf-8"),
            content_type="application/octet-stream",
        )
        return path

    def _request(self, method: str, endpoint: str, data: bytes | None = None,
                 content_type: str | None = None) -> None:
        headers = {"Authorization": f"Bearer {self.token}"}
        if content_type:
            headers["Content-Type"] = content_type
        request = urllib.request.Request(
            f"{self.host}{endpoint}", data=data, method=method, headers=headers
        )
        try:
            with urllib.request.urlopen(request, timeout=60):
                pass
        except urllib.error.HTTPError as error:
            detail = error.read().decode("utf-8", errors="replace")[:300]
            raise RuntimeError(f"Databricks {method} {endpoint} -> {error.code}: {detail}") from None


def _quote(path: str) -> str:
    return urllib.parse.quote(path, safe="/=")
