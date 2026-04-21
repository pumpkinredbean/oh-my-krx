"""Python-side hub indicator stub exposed to Monaco as an extra-lib.

This file is deliberately small and self-contained.  The admin SPA
reads it as a string at build time (``import ... ?raw``) and feeds it
into Monaco as completion fodder.  It is also used as the default
template source for the "new indicator" flow.
"""

from dataclasses import dataclass, field
from typing import Any, Iterable


@dataclass
class SeriesPoint:
    """Single datum emitted by a HubIndicator subclass.

    ``timestamp`` is an ISO-8601 UTC string; ``value`` is a number;
    ``meta`` is an optional JSON-serialisable dict.
    """

    timestamp: str
    value: float
    meta: dict[str, Any] = field(default_factory=dict)


class HubIndicator:
    """Base class every custom indicator must subclass.

    Class attrs (declare on the subclass):

    * ``name`` — human-readable name.
    * ``inputs`` — tuple of canonical event_type strings to forward
      into ``on_event``.  Known values: "trade", "order_book_snapshot",
      "program_trade", "ticker", "ohlcv", "mark_price",
      "funding_rate", "open_interest".
    * ``output_kind`` — "line" (default).

    ``on_event(event)`` is called for every matching event.  ``event``
    is a dict with:

    * ``event.event_type`` : str
    * ``event.symbol``     : str
    * ``event.market_scope`` : str ("" for non-KRX sources)
    * ``event.published_at`` : str (ISO-8601)
    * ``event.payload``    : dict   (canonical payload for that event_type)

    Return ``None``, a single ``SeriesPoint``, or a list of
    ``SeriesPoint`` objects.  Never import from os/sys/socket/etc. — the
    runtime rejects such scripts statically.
    """

    name: str = "indicator"
    inputs: tuple[str, ...] = ()
    output_kind: str = "line"

    def __init__(self, **params: Any) -> None:
        self.params: dict[str, Any] = dict(params)

    def on_event(self, event: dict[str, Any]) -> SeriesPoint | Iterable[SeriesPoint] | None:
        raise NotImplementedError


# ─── Templates ────────────────────────────────────────────────────────────

# Template: simple top-N order book imbalance.
class MyOBI(HubIndicator):
    name = "my_obi"
    inputs = ("order_book_snapshot",)
    output_kind = "line"

    def on_event(self, event):
        payload = event["payload"] or {}
        top_n = int(self.params.get("top_n") or 5)
        bids = [float(lvl[1]) for lvl in (payload.get("bids") or [])[:top_n]]
        asks = [float(lvl[1]) for lvl in (payload.get("asks") or [])[:top_n]]
        if not bids or not asks:
            return None
        denom = sum(bids) + sum(asks)
        if denom <= 0:
            return None
        return SeriesPoint(
            timestamp=event["published_at"],
            value=(sum(bids) - sum(asks)) / denom,
            meta={"top_n": top_n},
        )


# Template: VWAP sketch over trade stream.
class MyVWAP(HubIndicator):
    name = "my_vwap"
    inputs = ("trade",)
    output_kind = "line"

    def __init__(self, **params):
        super().__init__(**params)
        self._pv = 0.0
        self._vol = 0.0

    def on_event(self, event):
        payload = event["payload"] or {}
        price = float(payload.get("price") or 0)
        size = float(payload.get("size") or payload.get("quantity") or 0)
        if price <= 0 or size <= 0:
            return None
        self._pv += price * size
        self._vol += size
        if self._vol <= 0:
            return None
        return SeriesPoint(
            timestamp=event["published_at"],
            value=self._pv / self._vol,
        )
