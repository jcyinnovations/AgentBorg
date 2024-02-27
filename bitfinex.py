import logging
import time
import pendulum

from typing import List, Tuple, Any
from datetime import datetime, timezone
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
import requests
from utils import date_range

'''
logger = logging.getLogger()
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s %(levelname)-8s %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)
'''

API_URL = 'https://api.bitfinex.com/v2'

DEFAULT_START_DATE: int = datetime(2017, 1, 1, tzinfo=timezone.utc).timestamp()*1000

def requests_retry_session(
    url
    , retries=3
    , backoff_factor=1
    , status_forcelist=(429, 500, 502, 503, 504)
    , session=None
    , adapter=None
):
    """
    Configuration for `requests` retries.

    Args:
        url: url to get
        retries: total number of retry attempts
        backoff_factor: amount of time between attempts
        status_forcelist: retry if response is in list
        session: requests session object

    Example:
        req = requests_retry_session().get(<url>)
    """
    session = session or requests.Session()
    retry = Retry(
        total=retries
        ,backoff_factor=backoff_factor
        ,status_forcelist=status_forcelist
    )
    if adapter is None:
        adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session.get(url)


def get_candles(symbol, start_date, end_date, timeframe='1m', limit=1000, adapter=None):
    """
    Return symbol candles between two dates.
    https://docs.bitfinex.com/v2/reference#rest-public-candles
    """
    # timestamps need to include milliseconds
    #start_date = start_date.int_timestamp * 1000
    #end_date = end_date.int_timestamp * 1000

    url = f'{API_URL}/candles/trade:{timeframe}:t{symbol.upper()}/hist' \
          f'?start={start_date}&end={end_date}&limit={limit}'
    data = requests_retry_session(url, adapter=adapter).json()
    return data


def watchdog_timeout(timeout: int, timer_start: datetime):
    timer_interval = (datetime.now() - timer_start).total_seconds()
    return timeout and timer_interval >= timeout


def load_ticks_to_now(
    start_date: int=DEFAULT_START_DATE
    , end_date: int=None
    , symbol: str='btcusd'
    , timeout: int = None
    , adapter=None
) -> Tuple[bool, dict]:
    '''
    Load the most recent ticks from Bitfinex v2 API
    '''
    msg: bool = True

    if not end_date:
        end_date: int = pendulum.now().int_timestamp * 1000
    # 1000 minutes in milliseconds
    step: int = 1000*60*1000 
    timer_start: datetime = datetime.now()
    # Accumulate ticks before returning to calling job
    tick_store: List[Any] = [] 

    try:
        logging.info(f'{symbol} | Processing from {start_date}')
        for d1, d2 in date_range(start_date, end_date, step):
            logging.info(f'Fetching candles for dates: {d1} -> {d2}')
            # returns (max) 1000 candles, one for every minute
            candles = get_candles(symbol, d1, d2, adapter=adapter)
            logging.debug(f'Fetched {len(candles)} candles')
            if candles:
                # Use `extend` for performance and memory efficiency
                tick_store.extend(candles) 
            else:
                # Failed to load candles so abandon for now
                logging.error(f"Failed to load candles for {d1} -> {d2}")
                break
            # prevent from api rate-limiting
            time.sleep(3) 
            # Check for timeout
            if watchdog_timeout(timeout, timer_start):
                logging.info(f"Timeout {timeout} seconds reached. Quit loading ticks")
                break
    except:
        msg = False
        logging.exception(f"Failed to load_ticks() for date range: {pendulum.from_timestamp(start_date/1000)} - {pendulum.from_timestamp(end_date/1000)}")
    return msg, tick_store


def load_ticks_to_interval(
    d1
    , d2
    , symbols: List[str]=['btcusd']
) -> Tuple[bool, dict]:
    '''
    Load the most recent ticks from Bitfinex v2 API
    '''
    msg: bool = True
    tick_store = {s:[] for s in symbols} # Accumulate ticks before returning to calling job

    try:
        for i, symbol in enumerate(symbols, 1):
            logging.info(f'Fetching candles for {symbol} in date range: {d1} -> {d2}')
            # returns (max) 1000 candles, one for every minute
            candles = get_candles(symbol, d1, d2)
            logging.debug(f'Fetched {len(candles)} candles')
            if candles:
                tick_store[symbol].extend(candles)
            time.sleep(3) # prevent from api rate-limiting
    except:
        msg = False
        logging.exception(f"Failed to load_ticks() for date range: {pendulum.from_timestamp(d1/1000)} - {pendulum.from_timestamp(d2/1000)}")
    return msg, tick_store

