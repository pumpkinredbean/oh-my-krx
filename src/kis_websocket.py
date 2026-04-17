"""KIS realtime program-trade client — SUNSET CLI debug shim.

# SUNSET: This module is a shrunk debug shim for CLI-only program_trade access.
# KSXT v0.1.0 does not implement program_trade StreamKind; this file MUST be
# deleted and main.py rewritten to KSXT once KSXT program_trade lands.
# See FINAL-HUB-REPORT §6 follow-up and hub-D report.
"""
from __future__ import annotations

import json
import threading
from datetime import datetime, timedelta
from typing import Any, AsyncIterator

import pandas as pd
import requests
import websockets

from src.config import Settings


PROGRAM_TRADE_COLUMNS = [
    "MKSC_SHRN_ISCD", "STCK_CNTG_HOUR",
    "SELN_CNQN", "SELN_TR_PBMN", "SHNU_CNQN", "SHNU_TR_PBMN",
    "NTBY_CNQN", "NTBY_TR_PBMN",
    "SELN_RSQN", "SHNU_RSQN", "WHOL_NTBY_QTY",
]

DISPLAY_RENAME_MAP = {
    "MKSC_SHRN_ISCD": "종목코드", "STCK_CNTG_HOUR": "체결시각",
    "SELN_CNQN": "프로그램매도체결량", "SELN_TR_PBMN": "프로그램매도거래대금",
    "SHNU_CNQN": "프로그램매수체결량", "SHNU_TR_PBMN": "프로그램매수거래대금",
    "NTBY_CNQN": "프로그램순매수체결량", "NTBY_TR_PBMN": "프로그램순매수거래대금",
    "SELN_RSQN": "매도호가잔량", "SHNU_RSQN": "매수호가잔량",
    "WHOL_NTBY_QTY": "전체순매수호가잔량",
}

MARKET_TR_ID = {"krx": "H0STPGM0", "nxt": "H0NXPGM0", "total": "H0UNPGM0"}

SCHEMA_BY_TR_ID = {
    tr_id: {
        "event": "program_trade",
        "columns": PROGRAM_TRADE_COLUMNS,
        "rename_map": DISPLAY_RENAME_MAP,
        "numeric_columns": PROGRAM_TRADE_COLUMNS[2:],
    }
    for tr_id in MARKET_TR_ID.values()
}


class KISProgramTradeClient:
    def __init__(self, settings: Settings):
        self.settings = settings
        self._active_ws: Any | None = None
        self._access_token: str | None = None
        self._access_token_expires_at: datetime | None = None
        self._access_token_lock = threading.Lock()

    def _is_access_token_valid(self) -> bool:
        if not self._access_token:
            return False
        if self._access_token_expires_at is None:
            return True
        return datetime.utcnow() < (self._access_token_expires_at - timedelta(minutes=1))

    @staticmethod
    def _parse_access_token_expiry(body: dict[str, Any]) -> datetime | None:
        expires_at_text = str(body.get("access_token_token_expired") or "").strip()
        if expires_at_text:
            for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
                try:
                    return datetime.strptime(expires_at_text, fmt)
                except ValueError:
                    continue

        expires_in = body.get("expires_in")
        try:
            expires_in_seconds = int(expires_in)
        except (TypeError, ValueError):
            return None

        if expires_in_seconds <= 0:
            return None
        return datetime.utcnow() + timedelta(seconds=expires_in_seconds)

    def get_access_token(self) -> str:
        if self._is_access_token_valid():
            return str(self._access_token)

        with self._access_token_lock:
            if self._is_access_token_valid():
                return str(self._access_token)

        self.settings.require_kis_credentials()
        url = f"{self.settings.rest_url}/oauth2/tokenP"
        payload = {
            "grant_type": "client_credentials",
            "appkey": self.settings.app_key,
            "appsecret": self.settings.app_secret,
        }
        headers = {
            "Content-Type": "application/json",
            "Accept": "text/plain",
            "charset": "UTF-8",
        }

        response = requests.post(url, headers=headers, data=json.dumps(payload), timeout=15)
        try:
            response.raise_for_status()
        except requests.HTTPError as exc:
            try:
                body = response.json()
            except ValueError:
                body = response.text
            raise RuntimeError(f"KIS access token request failed: {body}") from exc

        body = response.json()
        access_token = body.get("access_token")
        if not access_token:
            raise RuntimeError(f"access_token 발급 실패: {body}")
        self._access_token = str(access_token)
        self._access_token_expires_at = self._parse_access_token_expiry(body)
        return access_token

    def get_approval_key(self) -> str:
        self.settings.require_kis_credentials()
        url = f"{self.settings.rest_url}/oauth2/Approval"
        payload = {
            "grant_type": "client_credentials",
            "appkey": self.settings.app_key,
            "secretkey": self.settings.app_secret,
        }
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "charset": "UTF-8",
        }

        response = requests.post(url, headers=headers, data=json.dumps(payload), timeout=15)
        response.raise_for_status()

        body = response.json()
        approval_key = body.get("approval_key")
        if not approval_key:
            raise RuntimeError(f"approval_key 발급 실패: {body}")
        return approval_key

    def build_subscribe_message(self, approval_key: str, symbol: str, tr_id: str) -> dict:
        return {
            "header": {
                "approval_key": approval_key,
                "content-type": "utf-8",
                "custtype": "P",
                "tr_type": "1",
            },
            "body": {
                "input": {
                    "tr_id": tr_id,
                    "tr_key": symbol,
                }
            },
        }

    def parse_realtime_frame(self, raw: str) -> tuple[str, pd.DataFrame]:
        parts = raw.split("|", 3)
        if len(parts) != 4:
            raise ValueError(f"예상하지 못한 실시간 메시지 형식: {raw}")
        encrypted_flag, tr_id, count_text, payload = parts
        if encrypted_flag == "1":
            raise RuntimeError(f"암호화된 메시지는 현재 샘플에서 처리하지 않음: tr_id={tr_id}")
        try:
            row_count = int(count_text)
        except ValueError as exc:
            raise ValueError(f"유효하지 않은 실시간 row count: {count_text}") from exc
        schema = SCHEMA_BY_TR_ID.get(tr_id)
        if not schema:
            raise ValueError(f"지원하지 않는 실시간 TR ID: {tr_id}")

        values = payload.split("^")
        columns = schema["columns"]
        column_count = len(columns)
        expected_value_count = row_count * column_count
        row_width = column_count
        if len(values) != expected_value_count:
            if len(values) > expected_value_count and row_count > 0 and len(values) % row_count == 0:
                row_width = len(values) // row_count
            else:
                raise ValueError(
                    f"컬럼 수가 맞지 않음: values={len(values)} columns={column_count} count={count_text}"
                )
        if len(values) % row_width != 0:
            raise ValueError(
                f"컬럼 수가 맞지 않음: values={len(values)} columns={column_count} count={count_text}"
            )

        rows = [
            values[index:index + row_width][:column_count]
            for index in range(0, len(values), row_width)
        ]
        frame = pd.DataFrame(rows, columns=columns)
        for col in schema["numeric_columns"]:
            frame[col] = pd.to_numeric(frame[col], errors="coerce")
        return schema["event"], frame.rename(columns=schema["rename_map"])

    async def _iter_realtime_frames(self, ws: Any) -> AsyncIterator[tuple[str, pd.DataFrame]]:
        async for raw in ws:
            if isinstance(raw, bytes):
                raw = raw.decode("utf-8", errors="replace")
            if raw.startswith("{"):
                try:
                    body = json.loads(raw)
                except json.JSONDecodeError:
                    print(f"[WARN] JSON 파싱 실패: {raw}")
                    continue
                tr_id = body.get("header", {}).get("tr_id")
                if tr_id == "PINGPONG":
                    await ws.pong(raw)
                    continue
                msg = body.get("body", {}).get("msg1")
                rt_cd = body.get("body", {}).get("rt_cd")
                if msg:
                    print(f"[SYS] rt_cd={rt_cd} tr_id={tr_id} msg={msg}")
                continue
            if raw and raw[0] in {"0", "1"}:
                yield self.parse_realtime_frame(raw)

    async def subscribe(self, symbol: str, market: str) -> AsyncIterator[pd.DataFrame]:
        market = market.lower()
        if market not in MARKET_TR_ID:
            raise ValueError(f"지원하지 않는 market: {market}")

        approval_key = self.get_approval_key()
        ws_url = f"{self.settings.ws_url}/tryitout"
        message = self.build_subscribe_message(
            approval_key=approval_key,
            symbol=symbol,
            tr_id=MARKET_TR_ID[market],
        )

        async with websockets.connect(ws_url, ping_interval=30, ping_timeout=30) as ws:
            self._active_ws = ws
            try:
                await ws.send(json.dumps(message))

                async for event_type, frame in self._iter_realtime_frames(ws):
                    if event_type == "program_trade":
                        yield frame
            finally:
                if self._active_ws is ws:
                    self._active_ws = None

    async def aclose(self) -> None:
        ws = self._active_ws
        if ws is not None:
            try:
                await ws.close()
            except Exception:
                pass
