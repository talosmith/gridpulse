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


class SinkError(RuntimeError):
    """The destination itself is unusable: retrying other files would fail the same way."""


def normalize_host(host: str) -> str:
    """Keep only scheme + domain, so a URL pasted from the browser address bar still works.

    'dbc-1.cloud.databricks.com', 'https://dbc-1.cloud.databricks.com/?o=42#browse'
    -> 'https://dbc-1.cloud.databricks.com'
    """
    host = host.strip()
    if "://" not in host:
        host = f"https://{host}"
    parsed = urllib.parse.urlsplit(host)
    if not parsed.netloc:
        raise SinkError("DATABRICKS_HOST is empty or not a URL")
    return f"https://{parsed.netloc}"


class DatabricksVolumeSink:
    """Uploads through the Databricks Files API (works on Free Edition).

    volume_root example: /Volumes/gridpulse/raw/landing
    """

    def __init__(self, host: str, token: str, volume_root: str):
        parts = volume_root.strip("/").split("/")
        if len(parts) < 4 or parts[0] != "Volumes":
            raise SinkError("volume_root must look like /Volumes/<catalog>/<schema>/<volume>[/subdir]")
        self.host = normalize_host(host)
        self.token = token.strip()
        self.volume_root = "/" + "/".join(parts)
        self.volume = "/" + "/".join(parts[:4])

    def check(self) -> None:
        """Fail fast, with a readable reason, before collecting anything."""
        try:
            self._request("HEAD", f"/api/2.0/fs/directories{_quote(self.volume)}")
        except SinkError as error:
            code = getattr(error, "code", None)
            hint = {
                401: "the token is invalid or expired (DATABRICKS_TOKEN)",
                403: "the token has no permission on this volume",
                404: (f"volume {self.volume} not found, or DATABRICKS_HOST is not the workspace URL "
                      f"(using {self.host.split('.')[0]}...)"),
            }.get(code, "unexpected answer")
            raise SinkError(f"Databricks preflight failed: {hint}. {error}") from None

    def trigger_pipeline(self, pipeline_id: str) -> None:
        """Start an update of the Lakeflow pipeline so new files flow to gold right away."""
        self._request(
            "POST",
            f"/api/2.0/pipelines/{urllib.parse.quote(pipeline_id.strip())}/updates",
            data=b"{}",
            content_type="application/json",
        )

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
            failure = SinkError(f"Databricks {method} {endpoint} -> {error.code}: {detail}")
            failure.code = error.code
            raise failure from None
        except urllib.error.URLError as error:
            raise SinkError(f"cannot reach {self.host}: {error.reason}") from None


def _quote(path: str) -> str:
    return urllib.parse.quote(path, safe="/=")
