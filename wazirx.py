"Track Arbitrage"
import pandas as pd
import requests
import io
from sqlalchemy import create_engine
from datetime import datetime
import os

import ccxt
import pandas as pd

profit_percentage = -1
exchange_id = 'wazirx'
exchange_class = getattr(ccxt, exchange_id)
private_api_exc = exchange_class()
# current_wrx_price = private_api_exc.fetch_ticker('wrx/INR')['last']
wrx_price = private_api_exc.fetch_ticker('WRX/INR')['last']


def process_data(_d):
    _d['inr-buy-wrx-conversion'] = _d.get('inrbuy', 0) / wrx_price
    _d['inr-sell-wrx-conversion'] = _d.get('inrsell', 0) / wrx_price
    _d['sell_difference_in_$'] = _d['wrxsell'] - _d['inr-sell-wrx-conversion']
    _d['buy_differnce_in_$'] = _d['wrxbuy'] - _d['inr-buy-wrx-conversion']
    _d['min_market_sell_price_$'] = min(_d['inr-sell-wrx-conversion'], _d['wrxsell'])
    _d['final_buy_pair'] = 'inr' if _d['inr-sell-wrx-conversion'] < _d['wrxsell'] else 'wrx'
    _d['final_sell_pair'] = 'inr' if _d['inr-buy-wrx-conversion'] > _d['wrxbuy'] else 'wrx'
    _d['max_market_buy_price_$'] = max(_d['inr-buy-wrx-conversion'], _d['wrxbuy'])
    _d['profit'] = ((_d['max_market_buy_price_$'] - _d['min_market_sell_price_$']) * 100 / _d[
        'min_market_sell_price_$']) - 0.2
    _d['indexed_time'] = str(datetime.now(tz))
    return _d

start = datetime.now()
r = requests.get('https://api.wazirx.com/api/v2/market-status')
end = datetime.now()
print('time_taken', str(end - start)[:10])
data_dict = enumerate(r.json().get('markets'))
tz = pytz.timezone('UTC')
df = pd.DataFrame()
d = []
d1 = {}
coins_list = []
for j, i in data_dict:
    if i['type'] == 'SPOT' and i['quoteMarket'] in ('inr', 'wrx') and i['status'] != 'suspended':
        if i.get('baseMarket') == 'wrx':
            wrx_price = float(i['sell'])
        d1[i['quoteMarket'] + 'index'] = j
        if not d1.get(i['baseMarket']):
            d1[i['baseMarket']] = {i['quoteMarket'] + 'sell': float(i['sell']),
                                    i['quoteMarket'] + 'buy': float(i['buy'])}
        else:
            d1[i['baseMarket']][i['quoteMarket'] + 'sell'] = float(i['sell'])
            d1[i['baseMarket']][i['quoteMarket'] + 'buy'] = float(i['buy'])
            d1['coin'] = i['baseMarket']
            if d1[i['baseMarket']]['inrsell'] > 0:
                _d = process_data(d1[i['baseMarket']])
                _d['coin'] = i['baseMarket']
                if _d['profit'] > profit_percentage:
                    coins_list.append(_d)

sorted_coins_list = sorted(coins_list, key=lambda i: i['profit'])
print(sorted_coins_list[0])

df = pd.DataFrame(sorted_coins_list)

def index_to_db(df):
    engine = create_engine(
        'postgresql+psycopg2://'+os.environ.get('arb_db_user')+':'+os.environ.get('arb_db_pass')+'@'+os.environ.get('arb_db_host')+':5432/'+os.environ.get('arb_db_name'))
    df.head(0).to_sql('wrx_inr_arbitrage', engine, if_exists='append',index=False)
    conn = engine.raw_connection()
    cur = conn.cursor()
    output = io.StringIO()
    df.to_csv(output, sep='\t', header=False, index=False)
    output.seek(0)
    contents = output.getvalue()
    cur.copy_from(output, 'wrx_inr_arbitrage', null="") # null values become ''
    conn.commit()
    cur.close()
    conn.close()
    engine.dispose()

index_to_db(df)