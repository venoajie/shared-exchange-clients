# src\shared_exchange_clients\mappers.py

from typing import Dict, Any
from loguru import logger as log # Make sure loguru is imported
from trading_engine_core.models.enums import MarketType

def get_canonical_market_type(
    exchange_name: str,
    raw_instrument: Dict[str, Any],
    source_hint: str = None
) -> MarketType:
    """
    Translates raw exchange data into the canonical MarketType enum.

    Args:
        exchange_name: The name of the exchange (e.g., 'deribit').
        raw_instrument: The raw JSON dictionary for the instrument from the exchange.
        source_hint: For exchanges like Binance where market type is implied by the
                     API endpoint, this provides the necessary context.

    Deribit API v_new fields:
        kind: "future", "option", "spot", "future_combo", "option_combo"
        instrument_type: "linear", "reversed" (for futures)
    """
    if exchange_name == "deribit":

        kind = raw_instrument.get("kind")
        instrument_type = raw_instrument.get("instrument_type")
           
        if instrument_type == "reversed": # "reversed" is Deribit's term for inverse
            if "future" in kind:
                if "combo" in kind:
                    return MarketType.INVERSE_FUTURES_COMBO
                else:
                    return MarketType.INVERSE_FUTURES

            if "option" in kind:
                if "combo" in kind:
                    return MarketType.INVERSE_OPTIONS_COMBO
                else:
                    return MarketType.INVERSE_OPTIONS

        elif instrument_type == "linear": 
            if "future" in kind:
                if "combo" in kind:
                    return MarketType.LINEAR_FUTURES_COMBO
                else:
                    return MarketType.LINEAR_FUTURES

            if "option" in kind:                
                if "combo" in kind:
                    return MarketType.LINEAR_OPTIONS_COMBO
                else:
                    return MarketType.LINEAR_OPTIONS

            if kind == "spot":
                return MarketType.SPOT

    elif exchange_name == "binance":
        if source_hint == MarketType.SPOT.value:
            return MarketType.SPOT
        if source_hint == MarketType.LINEAR_FUTURES.value:
            return MarketType.LINEAR_FUTURES
        if source_hint == MarketType.INVERSE_FUTURES.value:
            return MarketType.INVERSE_FUTURES

    # Default catch-all
    return MarketType.UNKNOWN