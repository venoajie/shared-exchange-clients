# src\shared_exchange_clients\trading\deribit\api_client.py

import orjson
from loguru import logger as log
from typing import Dict, Any, Optional
import aiohttp
import time

from .constants import ApiMethods


class DeribitApiClient:
    """
    A production-grade API client for interacting with the Deribit v2 API.
    Manages an aiohttp session and handles JSON-RPC 2.0 requests with authentication.
    """

    def __init__(
        self,
        client_id: str,
        client_secret: str,
    ):
        self.client_id = client_id
        self.client_secret = client_secret
        self._session: Optional[aiohttp.ClientSession] = None
        self._base_url = "https://www.deribit.com/api/v2"
        self._access_token: Optional[str] = None
        log.info("Deribit API client initialized for production use.")

    async def connect(self):
        """Establishes the aiohttp client session and authenticates."""
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                json_serialize=lambda data: orjson.dumps(data).decode()
            )
            log.info("Aiohttp session established for Deribit API.")
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

        auth_params = {
            "grant_type": "client_credentials",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
        }
        url = f"{self._base_url}/public/auth"
        log.info("Attempting to authenticate with Deribit...")
        try:
            async with self._session.get(url, params=auth_params) as response:
                response.raise_for_status()
                data = await response.json()
                if "result" in data and "access_token" in data["result"]:
                    self._access_token = data["result"]["access_token"]
                    log.success(
                        "Successfully authenticated with Deribit and obtained access token."
                    )
                else:
                    log.error(
                        f"Deribit authentication failed: {data.get('error', 'Unknown error')}"
                    )
                    self._access_token = None
        except aiohttp.ClientError as e:
            log.critical(f"HTTP error during authentication: {e}")
            self._access_token = None

    async def _make_request(
        self,
        method: str,
        params: Dict[str, Any],
    ) -> Dict[str, Any]:
        """A generic, internal method for making authenticated JSON-RPC requests."""
        if not method.startswith("public/") and not self._access_token:
            return {"success": False, "error": "Not authenticated"}
        if not self._session or self._session.closed:
            return {"success": False, "error": "Session is not active"}

        headers = {}
        if self._access_token:
            headers["Authorization"] = f"bearer {self._access_token}"

        payload = {
            "jsonrpc": "2.0",
            "id": int(time.time() * 1000),
            "method": method,
            "params": params,
        }

        try:
            async with self._session.post(
                self._base_url, json=payload, headers=headers
            ) as response:
                if not response.ok:
                    try:
                        error_data = await response.json()
                        log.error(
                            f"Deribit API Error for method {method}: HTTP {response.status}, Body: {error_data}"
                        )
                        return {
                            "success": False,
                            "error": error_data.get("error", str(error_data)),
                        }
                    except Exception:
                        error_text = await response.text()
                        log.error(
                            f"Deribit API Error for method {method}: HTTP {response.status}, Body: {error_text}"
                        )
                        return {
                            "success": False,
                            "error": f"HTTP {response.status}: {response.reason}",
                        }

                data = await response.json()
                if "error" in data:
                    log.error(f"Deribit API Error for method {method}: {data['error']}")
                    return {"success": False, "error": data["error"]}
                return {"success": True, "data": data.get("result")}
        except aiohttp.ClientError as e:
            log.error(f"HTTP request for method {method} failed: {e}")
            return {"success": False, "error": str(e)}
        except Exception as e:
            log.error(
                f"An unexpected error occurred during API request for {method}: {e}",
                exc_info=True,
            )
            return {"success": False, "error": "Unexpected error"}

    async def get_instruments(
        self,
        currency: str,
        kind: str = "future",
        expired: bool = False,
    ) -> Dict[str, Any]:
        """Fetches all instruments for a given currency and kind."""
        log.info(f"[API CALL] Fetching {kind} instruments for currency: {currency}")
        return await self._make_request(
            ApiMethods.GET_INSTRUMENTS,
            {
                "currency": currency,
                "kind": kind,
                "expired": expired,
            },
        )

    async def get_tradingview_chart_data(
        self,
        instrument_name: str,
        start_timestamp: int,
        end_timestamp: int,
        resolution: str,
    ) -> Dict[str, Any]:
        """Fetches OHLC data from the TradingView-compatible endpoint."""
        # log.info(
        #    f"[API CALL] Fetching chart data for {instrument_name} ({resolution}) from {start_timestamp} to {end_timestamp}"
        # )
        params = {
            "instrument_name": instrument_name,
            "start_timestamp": start_timestamp,
            "end_timestamp": end_timestamp,
            "resolution": resolution,
        }

        return await self._make_request(
            ApiMethods.GET_TRADINGVIEW_CHART_DATA,
            params,
        )

    async def cancel_order(
        self,
        order_id: str,
    ) -> Dict[str, Any]:
        """Sends a real 'cancel' request to the exchange."""
        log.info(f"[API CALL] Attempting to cancel order: {order_id}")
        response = await self._make_request(
            ApiMethods.CANCEL_ORDER, {"order_id": order_id}
        )
        if response["success"]:
            log.success(
                f"[API RESPONSE] Successfully processed cancel for order: {order_id}."
            )
        else:
            log.error(
                f"[API RESPONSE] Failed to cancel order {order_id}. Error: {response.get('error')}"
            )
        return response

    async def create_order(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Sends a real 'create order' request to the exchange."""
        client_order_id = params.get("label")
        log.info(
            f"[API CALL] Attempting to create order with client_id: {client_order_id}"
        )
        # REMEDIATION: The 'side' key is used to construct the endpoint but is not part of the API payload.
        # It must be removed from the params sent to the exchange.
        side = params.get("side")
        if not side:
            return {
                "success": False,
                "error": "Missing 'side' parameter for create_order",
            }

        # Create a copy of the params to avoid modifying the original dict.
        payload_params = params.copy()
        # Remove the 'side' key as it's not part of the Deribit API contract for this endpoint.
        payload_params.pop("side", None)

        endpoint = f"private/{side}"
        response = await self._make_request(endpoint, payload_params)
        if response["success"]:
            log.success(
                f"[API RESPONSE] Successfully processed create for order: {client_order_id}."
            )
        else:
            log.error(
                f"[API RESPONSE] Failed to create order for {client_order_id}. Error: {response.get('error')}"
            )
        return response

    async def create_oto_order(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Sends a request to create a paired OTO (One-Triggers-Other) order.
        The `params` are expected to contain the main order details and an `otoco_config`.
        """
        client_order_id = params.get("label")
        log.info(
            f"[API CALL] Attempting to create OTO order with client_id: {client_order_id}"
        )
        # REMEDIATION: The 'side' key is used to construct the endpoint but is not part of the API payload.
        # It must be removed from the params sent to the exchange.
        side = params.get("side")
        if not side:
            return {
                "success": False,
                "error": "Missing 'side' parameter for create_oto_order",
            }

        # Create a copy of the params to avoid modifying the original dict.
        payload_params = params.copy()
        # Remove the 'side' key as it's not part of the Deribit API contract for this endpoint.
        payload_params.pop("side", None)

        endpoint = f"private/{side}"
        response = await self._make_request(endpoint, payload_params)
        if response["success"]:
            log.success(
                f"[API RESPONSE] Successfully processed OTO create for order: {client_order_id}."
            )
        else:
            log.error(
                f"[API RESPONSE] Failed to create OTO order for {client_order_id}. Error: {response.get('error')}"
            )
        return response

    async def get_open_orders_by_currency(self, currency: str) -> Dict[str, Any]:
        """Fetches all open orders for a given currency."""
        log.info(f"[API CALL] Fetching open orders for currency: {currency}")
        return await self._make_request(
            ApiMethods.GET_OPEN_ORDERS_BY_CURRENCY, {"currency": currency}
        )

    async def get_account_summary(self, currency: str) -> Dict[str, Any]:
        """Fetches account summary, including equity."""
        log.info(f"[API CALL] Fetching account summary for currency: {currency}")
        return await self._make_request(
            ApiMethods.GET_ACCOUNT_SUMMARY, {"currency": currency}
        )

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
        log.info(
            f"[API CALL] Fetching transaction log for {currency} from {start_timestamp} to {end_timestamp}"
        )
        params = {
            "currency": currency,
            "start_timestamp": start_timestamp,
            "end_timestamp": end_timestamp,
            "count": count,
            "query": query,
        }
        if continuation:
            params["continuation"] = continuation
        return await self._make_request(ApiMethods.GET_TRANSACTION_LOG, params)

    async def get_user_trades_by_order(self, order_id: str) -> Dict[str, Any]:
        """Fetches user trades for a specific order ID."""
        log.debug(f"[API CALL] Fetching trades for order_id: {order_id}")
        return await self._make_request(
            ApiMethods.GET_USER_TRADES_BY_ORDER, {"order_id": order_id}
        )

    async def get_subaccounts_details(self, currency: str) -> Dict[str, Any]:
        """
        Fetches detailed subaccount information, including open orders and positions.
        """
        log.info(f"[API CALL] Fetching subaccount details for currency: {currency}")
        params = {"currency": currency, "with_open_orders": True}
        return await self._make_request(ApiMethods.GET_SUBACCOUNTS_DETAILS, params)

    async def simulate_pme(self, positions: Dict[str, float]) -> Dict[str, Any]:
        """
        Calls the private/pme/simulate endpoint to get official margin calculations.
        """
        log.info(
            f"[API CALL] Simulating PME for portfolio with {len(positions)} positions."
        )
        params = {
            "currency": "CROSS",
            "add_positions": True,
            "simulated_positions": positions,
        }
        return await self._make_request(ApiMethods.SIMULATE_PME, params)
