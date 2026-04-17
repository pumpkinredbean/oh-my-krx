"""KIS settings compatibility shim.

Historically this re-exported from ``packages.adapters.kis.config``. That
adapter package was removed in hub-D (see FINAL-HUB-REPORT §6 follow-up);
the small dataclass needed by ``src/kis_websocket.py`` (CLI debug shim) and
``apps/collector/service.py`` is inlined here to keep the compatibility
path working until KSXT ``program_trade`` StreamKind ships.
"""
from __future__ import annotations

import os
from dataclasses import dataclass

from packages.shared.config import ROOT_DIR


@dataclass(frozen=True)
class KISSettings:
    app_key: str
    app_secret: str
    hts_id: str
    rest_url: str
    ws_url: str

    def require_kis_credentials(self) -> None:
        missing = []
        if not self.app_key:
            missing.append("KIS_APP_KEY")
        if not self.app_secret:
            missing.append("KIS_APP_SECRET")
        if missing:
            raise ValueError(f".env에 다음 값을 입력해야 합니다: {', '.join(missing)}")


def load_kis_settings() -> KISSettings:
    _ = ROOT_DIR
    return KISSettings(
        app_key=os.getenv("KIS_APP_KEY", "").strip(),
        app_secret=os.getenv("KIS_APP_SECRET", "").strip(),
        hts_id=os.getenv("KIS_HTS_ID", "").strip(),
        rest_url=os.getenv("KIS_REST_URL", "https://openapi.koreainvestment.com:9443").strip(),
        ws_url=os.getenv("KIS_WS_URL", "ws://ops.koreainvestment.com:21000").strip(),
    )


Settings = KISSettings
load_settings = load_kis_settings
settings = load_kis_settings()


__all__ = ["ROOT_DIR", "Settings", "KISSettings", "load_settings", "load_kis_settings", "settings"]
