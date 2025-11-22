# src/services/janitor/exchange_clients/deribit_client.py

from typing import List, Dict, Any, Optional
from loguru import logger as log
import aiohttp
from datetime import datetime, timezone
import asyncio

from shared_exchange_clients.public.base_client import AbstractJanitorClient
from shared_config.config import settings
from shared_exchange_clients.mappers import get_canonical_market_type


def _transform_deribit_instrument(raw_instrument: Dict[str, Any]) -> Dict[str, Any]:
    """Transforms a single raw Deribit instrument into our canonical format."""
    instrument_name = raw_instrument.get("instrument_name")
    kind = raw_instrument.get("kind")

    if kind == "future":
        instrument_kind = "perpetual" if "PERPETUAL" in instrument_name else "future"
    elif kind == "option":
        instrument_kind = "option"
    else:
        instrument_kind = "unknown"

    exp_ts_ms = raw_instrument.get("expiration_timestamp")
    expiration_timestamp = (
        datetime.fromtimestamp(exp_ts_ms / 1000, tz=timezone.utc) if exp_ts_ms else None
    )
    canonical_market_type = get_canonical_market_type("deribit", raw_instrument)

    return {
        "exchange": "deribit",
        "instrument_name": instrument_name,
        "market_type": canonical_market_type.value,
        "instrument_kind": instrument_kind,
        "base_asset": raw_instrument.get("base_currency"),
        "quote_asset": raw_instrument.get("quote_currency"),
        "settlement_asset": raw_instrument.get("settlement_currency")
        or raw_instrument.get("base_currency"),
        "settlement_period": raw_instrument.get("settlement_period"),
        "tick_size": raw_instrument.get("tick_size"),
        "contract_size": raw_instrument.get("contract_size"),
        "expiration_timestamp": (
            expiration_timestamp.isoformat() if expiration_timestamp else None
        ),
        "data": raw_instrument,
    }


class DeribitJanitorClient(AbstractJanitorClient):
    """
    The Deribit REST client for Janitor tasks. This client is specifically for
    handling PUBLIC, unauthenticated API endpoints as used by the Janitor service.
    """

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self._session: Optional[aiohttp.ClientSession] = None

        self.rest_url = config.get("rest_url")
        if not self.rest_url:
            raise ValueError("Deribit REST API URL ('rest_url') not configured.")

    async def get_public_trades(
        self,
        instrument: str,
        start_ts: int,
        end_ts: int,
        market_type: str,
    ) -> List[Dict[str, Any]]:
        """Implement public trades fetching for Deribit"""
        log.info(f"Fetching public trades for {instrument} from {start_ts} to {end_ts}")
        all_trades = []
        current_start_ts = start_ts

        while current_start_ts < end_ts:
            params = {
                "instrument_name": instrument,
                "start_timestamp": current_start_ts,
                "end_timestamp": end_ts,
                "count": 1000,  # Max allowed by Deribit
                "sorting": "asc",
            }

            result = await self._public_request(
                "public/get_last_trades_by_instrument", params
            )

            trades = result.get("trades", [])
            if not trades:
                break

            for trade in trades:
                all_trades.append(
                    {
                        "exchange": "deribit",
                        "instrument_name": instrument,
                        "market_type": market_type,
                        "trade_id": trade.get("trade_id", ""),
                        "price": trade.get("price", 0),
                        "quantity": trade.get("amount", 0),
                        "timestamp": datetime.fromtimestamp(
                            trade["timestamp"] / 1000, tz=timezone.utc
                        ),
                        "is_buyer_maker": trade.get("direction", "") == "sell",
                        "was_best_price_match": None,  # Not provided by Deribit
                    }
                )

            last_trade_ts = trades[-1]["timestamp"]
            if last_trade_ts >= end_ts:
                break

            current_start_ts = last_trade_ts + 1
            await asyncio.sleep(0.2)  # Rate limiting

        log.info(f"Fetched {len(all_trades)} public trades for {instrument}")
        return all_trades

    async def connect(self):
        """Establishes the client session. No login is required."""
        self._session = aiohttp.ClientSession()
        log.info("[DeribitJanitorClient] Public aiohttp session established.")

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()

    async def _public_request(
        self,
        endpoint: str,
        params: dict = None,
    ):
        """Helper for making a public request to Deribit."""
        url = f"{self.rest_url}/{endpoint}"
        try:
            async with self._session.get(url, params=params, timeout=20) as response:
                response.raise_for_status()
                data = await response.json()
                return data.get("result", [])
        except Exception as e:
            log.error(
                "Failed to fetch from Deribit public endpoint {}: {}", endpoint, e
            )
            return {}

    async def get_instruments(self) -> List[Dict[str, Any]]:
        """
        Fetches all relevant instruments by looping through the configured currencies
        and kinds internally.
        """
        all_canonical_instruments = []

        # The client is now responsible for its own looping logic.
        for currency in settings.hedged_currencies:
            for kind in ["future", "option"]:
                params = {"currency": currency, "kind": kind, "expired": "false"}
                raw_instruments = await self._public_request(
                    "public/get_instruments", params
                )

                if raw_instruments:
                    transformed = [
                        _transform_deribit_instrument(inst) for inst in raw_instruments
                    ]
                    all_canonical_instruments.extend(transformed)
                    log.info(
                        f"[DeribitJanitorClient] Fetched and transformed {len(transformed)} {kind} instruments for {currency}."
                    )
                # Add a small delay to be respectful to the API
                await asyncio.sleep(0.2)

        return all_canonical_instruments

    async def get_historical_ohlc(
        self,
        instrument: str,
        start_ts: int,
        end_ts: int,
        resolution: str,
        market_type: str,  # Unused for Deribit but required by abstract method
    ) -> Dict[str, Any]:
        params = {
            "instrument_name": instrument,
            "start_timestamp": start_ts,
            "end_timestamp": end_ts,
            "resolution": resolution,
        }
        response = await self._public_request(
            "public/get_tradingview_chart_data", params
        )
        return response
