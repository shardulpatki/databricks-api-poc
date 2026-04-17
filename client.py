"""Thin wrapper around the Databricks REST API.

The whole point of this file is to make the auth story obvious:
a Personal Access Token (PAT) is attached ONCE to a requests.Session,
and every subsequent call reuses that session.
"""

from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any, Optional

import requests


class DatabricksClient:
    """Authenticated HTTP client for the Databricks REST API."""

    def __init__(self, host: Optional[str] = None, token: Optional[str] = None, record: bool = False):
        # Env vars are the canonical source; explicit args are here mainly for tests.
        host = host or os.getenv("DATABRICKS_HOST")
        token = token or os.getenv("DATABRICKS_TOKEN")

        missing = [name for name, val in (("DATABRICKS_HOST", host), ("DATABRICKS_TOKEN", token)) if not val]
        if missing:
            raise RuntimeError(
                f"Missing required environment variable(s): {', '.join(missing)}. "
                "Set them in your shell or in a .env file (see .env.example)."
            )

        # Normalize host: ensure scheme, drop trailing slash.
        host = host.strip().rstrip("/")
        if not host.startswith(("http://", "https://")):
            host = "https://" + host
        self.host = host

        # ---- THE AUTH MOMENT ----
        # A requests.Session keeps headers across calls. By putting the bearer
        # token on the session once, every GET/POST we issue below automatically
        # carries "Authorization: Bearer <token>". No per-call plumbing needed.
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
        })

        self.record = record
        self.calls: list[dict[str, Any]] = []

    def _record(self, method: str, path: str, params: Optional[dict], response: requests.Response, body: Any) -> None:
        if not self.record:
            return
        self.calls.append({
            "method": method,
            "path": path,
            "params": params,
            "status": response.status_code,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "body": body,
        })

    def get(self, path: str, params: Optional[dict] = None) -> dict[str, Any]:
        """GET {host}{path} and return parsed JSON.

        Raises requests.HTTPError on non-2xx responses so callers can handle
        auth/permission failures centrally.
        """
        url = f"{self.host}{path}"
        response = self.session.get(url, params=params, timeout=30)
        response.raise_for_status()
        body = response.json()
        self._record("GET", path, params, response, body)
        return body

    def get_text(self, path: str) -> str:
        """GET {host}{path} and return the raw text body.

        Used for endpoints that return Prometheus/OpenMetrics text
        (e.g. /api/2.0/serving-endpoints/{name}/metrics) rather than JSON.
        """
        url = f"{self.host}{path}"
        response = self.session.get(url, headers={"Accept": "text/plain"}, timeout=30)
        response.raise_for_status()
        self._record("GET", path, None, response, response.text)
        return response.text

    def post(self, path: str, json: Optional[dict] = None) -> dict[str, Any]:
        """POST JSON to {host}{path} and return parsed JSON."""
        url = f"{self.host}{path}"
        response = self.session.post(url, json=json or {}, timeout=30)
        response.raise_for_status()
        body = response.json() if response.content else {}
        self._record("POST", path, None, response, body)
        return body
