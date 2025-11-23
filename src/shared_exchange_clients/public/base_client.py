# src/shared_exchange_clients/public/base_client.py

from abc import ABC, abstractmethod
from typing import List, Dict, Any


class AbstractJanitorClient(ABC):
    """
    Defines the standard interface for a REST API client used by the Janitor.
    """

    def __init__(self, config: Dict[str, Any]):
        self.config = config

    @abstractmethod
    async def connect(self):
        """Establishes the client session and handles authentication."""
        pass

    @abstractmethod
    async def close(self):
        """Closes the client session."""
        pass

    @abstractmethod
    async def get_instruments(self) -> List[Dict[str, Any]]:
        """Fetches all relevant instruments for the exchange."""
        pass

    @abstractmethod
    async def get_historical_ohlc(
        self,
        instrument: str,
        start_ts: int,
        end_ts: int,
        resolution: str,
        market_type: str,
    ) -> Dict[str, Any]:
        """Fetches historical OHLC data for an instrument."""
        pass

    @abstractmethod
    async def get_public_trades(
        self,
        instrument: str,
        start_ts: int,
        end_ts: int,
        market_type: str,
    ) -> List[Dict[str, Any]]:
        """
        [NEW] Fetches historical PUBLIC trades for a given instrument. This is for
        market data analysis, not private account auditing.
        """
        pass
