# src\shared_exchange_clients\trading\deribit\constants.py

"""
Static application constants
"""


class ApiMethods:
    """Centralizes the string names for API endpoints."""

    # Public
    GET_INSTRUMENTS = "public/get_instruments"
    GET_TRADINGVIEW_CHART_DATA = "public/get_tradingview_chart_data"

    # Private
    GET_ACCOUNT_SUMMARY = "private/get_account_summary"
    GET_TRANSACTION_LOG = "private/get_transaction_log"
    GET_SUBACCOUNTS_DETAILS = "private/get_subaccounts_details"
    GET_OPEN_ORDERS_BY_CURRENCY = "private/get_open_orders_by_currency"
    GET_USER_TRADES_BY_ORDER = "private/get_user_trades_by_order"
    CANCEL_ORDER = "private/cancel"
    SIMULATE_PME = "private/pme/simulate"
