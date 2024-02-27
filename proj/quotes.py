# python -c "import examples.rest.public.get_candles_hist"

from bfxapi import Client
from datetime import datetime, timezone

bfx = Client()
end = str(int(datetime.now(timezone.utc).timestamp() ) * 1000)
start = '1704145260000'

candles = bfx.rest.public.get_candles_hist(symbol='tBTCUSD', start='1704145260000', end=end, sort=-1, tf='1m')

for candle in candles:
    print(f"Candle: {candle}, volume type: {type(candle.volume)}")

#print(f"Candles: {bfx.rest.get_public_candles(symbol='tBTCUSD', start=start, end=end)}")

# Be sure to specify a period or aggregated period when retrieving funding candles.
# If you wish to mimic the candles found in the UI, use the following setup
#     to aggregate all funding candles: a30:p2:p30
# print(f"Candles: {bfx.rest.public.get_candles_hist(tf='1m', symbol='fUSD:a30:p2:p30')}")