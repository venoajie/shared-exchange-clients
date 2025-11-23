# src/services/janitor/exchange_clients/binance_client.py

from typing import List, Dict, Any
import aiohttp
from loguru import logger as log
import asyncio
from datetime import datetime, timezone

from shared_exchange_clients.public.base_client import AbstractJanitorClient
from shared_exchange_clients.mappers import get_canonical_market_type


def _transform_binance_instrument(
    raw_instrument: Dict[str, Any],
    market_type_hint: str,
) -> Dict[str, Any]:
    """Transforms a single raw Binance instrument from any market into our canonical format."""

    def get_tick_size(filters: list) -> float | None:
        for f in filters:
            if f.get("filterType") == "PRICE_FILTER":
                return float(f.get("tickSize", 0.0))
        return None

    contract_type = raw_instrument.get("contractType")

    canonical_market_type = get_canonical_market_type(
        "binance", raw_instrument, source_hint=market_type_hint
    )
    market_type = canonical_market_type.value

    if market_type == "spot":
        instrument_kind = "spot"
        settlement_asset = raw_instrument.get("quoteAsset")
    elif market_type == "linear_futures":
        instrument_kind = "perpetual" if contract_type == "PERPETUAL" else "future"
        settlement_asset = raw_instrument.get("marginAsset")
    elif market_type == "inverse_futures":
        instrument_kind = "perpetual" if contract_type == "PERPETUAL" else "future"
        settlement_asset = raw_instrument.get("marginAsset")
    else:
        instrument_kind = "unknown"
        settlement_asset = None  # This case should ideally not be hit

    # A final safety check to prevent database errors.
    if not settlement_asset:
        log.warning(
            f"Could not determine settlement_asset for {raw_instrument.get('symbol')} in market {market_type}. Skipping instrument."
        )
        return None  # Return None to filter this record out

    exp_ts_ms = raw_instrument.get("deliveryDate")
    expiration_timestamp = (
        datetime.fromtimestamp(exp_ts_ms / 1000, tz=timezone.utc) if exp_ts_ms else None
    )

    return {
        "exchange": "binance",
        "instrument_name": raw_instrument.get("symbol"),
        "market_type": market_type,
        "instrument_kind": instrument_kind,
        "base_asset": raw_instrument.get("baseAsset"),
        "quote_asset": raw_instrument.get("quoteAsset"),
        "settlement_asset": settlement_asset,
        "settlement_period": None,  # Binance API doesn't provide this directly
        "tick_size": get_tick_size(raw_instrument.get("filters", [])),
        "contract_size": raw_instrument.get("contractSize"),
        "expiration_timestamp": (
            expiration_timestamp.isoformat() if expiration_timestamp else None
        ),
        "data": raw_instrument,
    }


class BinanceJanitorClient(AbstractJanitorClient):
    """The Binance REST client for Janitor tasks. It queries all market types."""

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self._session: aiohttp.ClientSession | None = None

    async def connect(self):
        self._session = aiohttp.ClientSession()
        log.info("[BinanceJanitorClient] Aiohttp session established.")

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()

    async def _get_exchange_info(self, url: str) -> List[Dict[str, Any]]:
        """Generic helper to fetch exchange info from a given base URL."""
        try:
            async with self._session.get(url, timeout=20) as response:
                response.raise_for_status()
                data = await response.json()
                return data.get("symbols", [])
        except Exception as e:
            log.error("Failed to fetch exchange info from {}: {}", url, e)
            return []

    async def get_instruments(
        self,
        currency: str = None,
        kind: str = None,
    ) -> List[Dict[str, Any]]:
        markets_to_query = {
            "spot": "https://api.binance.com/api/v3/exchangeInfo",
            "linear_futures": "https://fapi.binance.com/fapi/v1/exchangeInfo",
            "inverse_futures": "https://dapi.binance.com/dapi/v1/exchangeInfo",
        }

        tasks = {
            market_type: self._get_exchange_info(full_url)
            for market_type, full_url in markets_to_query.items()
        }

        results = await asyncio.gather(*tasks.values())

        all_canonical_instruments = []
        market_keys = list(tasks.keys())

        for i, raw_instruments in enumerate(results):
            market_type = market_keys[i]
            if raw_instruments:
                transformed = [
                    t
                    for t in (
                        _transform_binance_instrument(inst, market_type)
                        for inst in raw_instruments
                    )
                    if t is not None
                ]
                all_canonical_instruments.extend(transformed)
                log.info(
                    f"[BinanceJanitorClient] Fetched and transformed {len(transformed)} instruments for market '{market_type}'."
                )

        log.success(
            f"[BinanceJanitorClient] Total canonical instruments processed: {len(all_canonical_instruments)}"
        )
        return all_canonical_instruments

    async def get_historical_ohlc(
        self,
        instrument: str,
        start_ts: int,
        end_ts: int,
        resolution: str,
        market_type: str,  # This parameter is now critical
    ) -> Dict[str, Any]:
        """
        Fetches historical OHLC data from the correct Binance market API based on the
        instrument's known market type.
        """
        market_api_map = {
            "spot": ("https://api.binance.com/api/v3", "/klines"),
            "linear_futures": ("https://fapi.binance.com/fapi/v1", "/klines"),
            "inverse_futures": ("https://dapi.binance.com/dapi/v1", "/klines"),
        }

        base_url, endpoint = market_api_map.get(market_type, (None, None))

        if not base_url:
            log.error(
                f"Cannot fetch OHLC for '{instrument}': unsupported market_type '{market_type}'."
            )
            return {}

        resolution_map = {"1": "1m", "5": "5m", "15": "15m", "60": "1h", "1D": "1d"}
        binance_res = resolution_map.get(resolution)
        if not binance_res:
            raise ValueError(f"Unsupported resolution for Binance: {resolution}")

        params = {
            "symbol": instrument,
            "interval": binance_res,
            "startTime": start_ts,
            "endTime": end_ts,
            "limit": 1500,
        }

        try:
            url = f"{base_url}{endpoint}"
            log.debug(f"Requesting OHLC from: {url} with params: {params}")
            async with self._session.get(url, params=params, timeout=30) as response:
                response.raise_for_status()
                data = await response.json()
                return {
                    "ticks": [row[0] for row in data],
                    "open": [float(row[1]) for row in data],
                    "high": [float(row[2]) for row in data],
                    "low": [float(row[3]) for row in data],
                    "close": [float(row[4]) for row in data],
                    "volume": [float(row[5]) for row in data],
                }
        except aiohttp.ClientResponseError as e:
            log.error(
                f'Failed to fetch OHLC from {url} for {instrument}: {e.status}, message="{e.message}"'
            )
            return {}
        except Exception as e:
            log.error(f"Failed to fetch OHLC from {url} for {instrument}: {e}")
            return {}

    async def get_public_trades(
        self,
        instrument: str,
        start_ts: int,
        end_ts: int,
        market_type: str,
    ) -> List[Dict[str, Any]]:
        """
        [NEW] Fetches historical public trades for a given instrument using the
        aggTrades endpoint. Handles pagination to retrieve all trades in the
        given time window.
        """
        market_api_map = {
            "spot": "https://api.binance.com/api/v3",
            "linear_futures": "https://fapi.binance.com/fapi/v1",
            "inverse_futures": "https://dapi.binance.com/dapi/v1",
        }
        base_url = market_api_map.get(market_type)
        if not base_url:
            log.error(
                f"Cannot fetch public trades for '{instrument}': unsupported market_type '{market_type}'."
            )
            return []

        url = f"{base_url}/aggTrades"
        all_trades = []
        current_start_ts = start_ts

        while current_start_ts < end_ts:
            params = {
                "symbol": instrument,
                "startTime": current_start_ts,
                "endTime": end_ts,
                "limit": 1000,  # Max limit for aggTrades
            }
            try:
                async with self._session.get(
                    url, params=params, timeout=30
                ) as response:
                    response.raise_for_status()
                    trades = await response.json()

                    if not trades:
                        # No more trades in this time range
                        break

                    for trade in trades:
                        # Add market_type fallback
                        trade_market_type = market_type or "spot"
                        all_trades.append(
                            {
                                "exchange": "binance",
                                "instrument_name": instrument,
                                "market_type": trade_market_type,  # Pass market_type through for DB insertion
                                "trade_id": str(trade["a"]),  # Aggregated trade ID
                                "price": float(trade["p"]),
                                "quantity": float(trade["q"]),
                                "timestamp": datetime.fromtimestamp(
                                    trade["T"] / 1000, tz=timezone.utc
                                ),
                                "is_buyer_maker": trade["m"],
                                "was_best_price_match": trade["M"],
                            }
                        )

                    # Move to the next time window, avoiding duplicates
                    last_trade_ts = trades[-1]["T"]
                    current_start_ts = last_trade_ts + 1

                    # Respect API rate limits
                    await asyncio.sleep(0.2)

            except aiohttp.ClientResponseError as e:
                log.error(
                    f'Failed to fetch public trades from {url} for {instrument}: {e.status}, message="{e.message}"'
                )
                break  # Stop trying on API error
            except Exception as e:
                log.error(
                    f"Failed to fetch public trades from {url} for {instrument}: {e}"
                )
                break  # Stop trying on other errors

        log.info(
            f"Fetched {len(all_trades)} public trades for {instrument} from {start_ts} to {end_ts}"
        )
        return all_trades
