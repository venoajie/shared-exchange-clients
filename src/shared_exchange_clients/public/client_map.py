# src/shared_exchange_clients/public/client_map.py

from shared_exchange_clients.public.deribit_client import DeribitJanitorClient
from shared_exchange_clients.public.binance_client import BinanceJanitorClient

CLIENT_MAP = {
    "deribit": DeribitJanitorClient,
    "binance": BinanceJanitorClient,
}
