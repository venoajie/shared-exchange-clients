# src/shared_exchange_clients/trading/deribit/api_client.py

import asyncio
import orjson
from loguru import logger as log
from typing import Dict, Any, Optional
import aiohttp
import time
from functools import wraps

from .constants import ApiMethods

class TokenExpiredError(Exception):
    pass


class DeribitApiClient:
    """
    A production-grade API client for interacting with the Deribit v2 API.
    Manages an aiohttp session and handles JSON-RPC 2.0 requests with authentication.
    [MODIFIED] Now includes automatic, thread-safe token refresh logic.
    """

    def __init__(self, client_id: str, client_secret: str):
        self.client_id = client_id
        self.client_secret = client_secret
        self._session: Optional[aiohttp.ClientSession] = None
        self._base_url = "https://www.deribit.com/api/v2"
        self._access_token: Optional[str] = None
        self._auth_lock = asyncio.Lock()
        log.info("Deribit API client initialized for production use.")

    def _ensure_authenticated(func):
        """
        Decorator that transparently handles token expiration and retries the
        decorated function once upon failure.
        """
        @wraps(func)
        async def wrapper(self, *args, **kwargs):
            try:
                # First attempt to execute the function
                return await func(self, *args, **kwargs)
            except TokenExpiredError:
                # If the token is expired, acquire the lock to prevent multiple concurrent re-logins
                async with self._auth_lock:
                    log.warning(f"Access token expired during call to '{func.__name__}'. Re-authenticating.")
                    await self.login()
                    log.info(f"Re-authentication successful. Retrying call to '{func.__name__}'.")
                    # Second and final attempt after re-login
                    return await func(self, *args, **kwargs)
        return wrapper

    # --- CORE CONNECTION AND AUTH METHODS (NOT DECORATED) ---

    async def connect(self):
        """Establishes the aiohttp client session and authenticates."""
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                json_serialize=lambda data: orjson.dumps(data).decode()
            )
            log.info("Aiohttp session established for Deribit API.")
            # Initial login on connect
            await self.login()

    async def close(self):
        """Gracefully closes the aiohttp client session."""
        if self._session and not self._session.closed:
            await self._session.close()
            log.info("Aiohttp session for Deribit API closed.")

    async def login(self):
        """Authenticates with the Deribit API to get a bearer token."""
        if not self._session:
            raise ConnectionError("Session not established. Call connect() first.")
        
        auth_params = {"grant_type": "client_credentials", "client_id": self.client_id, "client_secret": self.client_secret}
        url = f"{self._base_url}/public/auth"
        log.info("Attempting to authenticate with Deribit...")
        try:
            async with self._session.get(url, params=auth_params) as response:
                response.raise_for_status()
                data = await response.json()
                if "result" in data and "access_token" in data["result"]:
                    self._access_token = data["result"]["access_token"]
                    log.success("Successfully authenticated with Deribit and obtained access token.")
                else:
                    log.error(f"Deribit authentication failed: {data.get('error', 'Unknown error')}")
                    self._access_token = None
        except aiohttp.ClientError as e:
            log.critical(f"HTTP error during authentication: {e}")
            self._access_token = None

    # --- SINGLE, ROBUST INTERNAL REQUEST METHOD ---
    
    async def _perform_request(self, method: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Internal helper to execute a single API request.
        This method is now the single point of failure and exception generation.
        """
        if not method.startswith("public/") and not self._access_token:
            raise ConnectionError("Not authenticated for private API call.")
        if not self._session or self._session.closed:
            raise ConnectionError("Session is not active.")

        headers = {"Authorization": f"bearer {self._access_token}"} if self._access_token else {}
        payload = {"jsonrpc": "2.0", "id": int(time.time() * 1000), "method": method, "params": params}

        async with self._session.post(self._base_url, json=payload, headers=headers) as response:
            data = await response.json()
            if response.status == 400 and data.get("error", {}).get("code") == 13009:
                raise TokenExpiredError(f"Token expired for method {method}")
            
            response.raise_for_status() # Raise exceptions for other HTTP errors (e.g., 500)
            
            if "error" in data:
                log.error(f"Deribit API Error for method {method}: {data['error']}")
                return {"success": False, "error": data["error"]}
            
            return {"success": True, "data": data.get("result")}

    # --- PUBLIC API METHODS (DECORATED) ---
            
    async def get_instruments(
        self,
        currency: str,
        kind: str = "future",
        expired: bool = False,
    ) -> Dict[str, Any]:
        """Fetches all instruments for a given currency and kind."""
        log.info(f"[API CALL] Fetching {kind} instruments for currency: {currency}")

        return await self._perform_request(
            ApiMethods.GET_INSTRUMENTS,
            {"currency": currency, "kind": kind, "expired": expired},
        )

    async def get_tradingview_chart_data(
        self,
        instrument_name: str,
        start_timestamp: int,
        end_timestamp: int,
        resolution: str,
    ) -> Dict[str, Any]:
        """Fetches OHLC data from the TradingView-compatible endpoint."""
        params = {
            "instrument_name": instrument_name,
            "start_timestamp": start_timestamp,
            "end_timestamp": end_timestamp,
            "resolution": resolution,
        }
        return await self._perform_request(
            ApiMethods.GET_TRADINGVIEW_CHART_DATA, params
        )

    @_ensure_authenticated
    async def cancel_order(self, order_id: str) -> Dict[str, Any]:
        log.info(f"[API CALL] Attempting to cancel order: {order_id}")
        return await self._perform_request(ApiMethods.CANCEL_ORDER, {"order_id": order_id})
    
    @_ensure_authenticated
    async def create_order(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Sends a real 'create order' request to the exchange."""
        # ... (implementation is the same, but calls _perform_request) ...
        side = params.get("side")
        endpoint = f"private/{side}"
        payload_params = params.copy()
        payload_params.pop("side", None)
        payload_params.pop("cycle_id", None)
        return await self._perform_request(endpoint, payload_params)
    
    @_ensure_authenticated
    async def create_oto_order(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Sends a request to create a paired OTO (One-Triggers-Other) order.
        """
        client_order_id = params.get("label")
        log.info(f"[API CALL] Attempting to create OTO order with client_id: {client_order_id}")
        
        side = params.get("side")
        if not side:
            return {"success": False, "error": "Missing 'side' parameter for create_oto_order"}

        payload_params = params.copy()
        payload_params.pop("side", None)
        payload_params.pop("cycle_id", None)
        
        endpoint = f"private/{side}"

        response = await self._perform_request(endpoint, payload_params)
        
        if response.get("success"):
            log.success(f"[API RESPONSE] Successfully processed OTO create for order: {client_order_id}.")
        else:
            log.error(f"[API RESPONSE] Failed to create OTO order for {client_order_id}. Error: {response.get('error')}")
        return response

    @_ensure_authenticated
    async def get_open_orders_by_currency(self, currency: str) -> Dict[str, Any]:
        """Fetches all open orders for a given currency."""
        log.info(f"[API CALL] Fetching open orders for currency: {currency}")

        return await self._perform_request(
            ApiMethods.GET_OPEN_ORDERS_BY_CURRENCY, {"currency": currency}
        )
        
    @_ensure_authenticated
    async def get_account_summary(self, currency: str) -> Dict[str, Any]:
        """Fetches account summary, including equity."""
        log.info(f"[API CALL] Fetching account summary for currency: {currency}")

        return await self._perform_request(
            ApiMethods.GET_ACCOUNT_SUMMARY, {"currency": currency}
        )

    @_ensure_authenticated
    async def get_transaction_log(
        self,
        currency: str,
        start_timestamp: int,
        end_timestamp: int,
        count: int = 1000,
        query: str = "trade",
        continuation: str = None,
    ) -> Dict[str, Any]:
        """Fetches the transaction log for a given period."""
        log.info(f"[API CALL] Fetching transaction log for {currency} from {start_timestamp} to {end_timestamp}")
        params = {
            "currency": currency,
            "start_timestamp": start_timestamp,
            "end_timestamp": end_timestamp,
            "count": count,
            "query": query,
        }
        if continuation:
            params["continuation"] = continuation

        return await self._perform_request(ApiMethods.GET_TRANSACTION_LOG, params)

    @_ensure_authenticated
    async def get_user_trades_by_order(self, order_id: str) -> Dict[str, Any]:
        """Fetches user trades for a specific order ID."""
        log.debug(f"[API CALL] Fetching trades for order_id: {order_id}")

        return await self._perform_request(
            ApiMethods.GET_USER_TRADES_BY_ORDER, {"order_id": order_id}
        )

    @_ensure_authenticated
    async def simulate_pme(self, positions: Dict[str, float]) -> Dict[str, Any]:
        """
        Calls the private/pme/simulate endpoint to get official margin calculations.
        """
        log.info(f"[API CALL] Simulating PME for portfolio with {len(positions)} positions.")
        params = {
            "currency": "CROSS",
            "add_positions": True,
            "simulated_positions": positions,
        }

        return await self._perform_request(ApiMethods.SIMULATE_PME, params)