import os
import time
import traceback
from datetime import datetime
import math
import ccxt
import pytz
import requests

print("execution_time",datetime.now())
public_api_requests = 0
private_api_requests = 0
profit_percentage = 1.2
price_correction_rate = 1.002
min_usdt_balance = 3
current_usdt_price = 0
exchange_id = 'wazirx'
exchange_class = getattr(ccxt, exchange_id)
private_api_exc = exchange_class({
    'apiKey': os.environ.get('wrx_access_keys'),
    'secret': os.environ.get('wrx_secret_keys'),
})
# private_api_exc = ccxt.wazirx()

# request - 1
primary_balance = private_api_exc.fetch_free_balance()
private_api_requests += 1


def timeit(method):
    """
    decorator to print the time taken by function in logs
    """

    def timed(*args, **kw):
        """
        decorator to print the time taken by function in logs
        """
        ts = time.time()
        result = method(*args, **kw)
        te = time.time()
        if 'log_time' in kw:
            name = kw.get('log_name', method.__name__.upper())
            kw['log_time'][name] = int((te - ts) * 1000)
        else:
            print(method.__name__, (te - ts), ' - seconds')
        return result

    return timed


@timeit
def get_price_for_required_quantity(type, coin_pair, quantity):
    global public_api_requests
    order_book = private_api_exc.fetch_order_book(coin_pair, limit=5)
    public_api_requests += 1
    bid_or_ask = 'bids' if type == 'sell' else 'asks'
    sum = 0
    for i in order_book[bid_or_ask]:
        sum += i[1]
        if sum >= quantity:
            return i[0]
    return 0


@timeit
def maintain_min_inr_usdt_balance(current_balance):
    global public_api_requests
    global private_api_requests
    '''need to maintain around equal balance in both
    inr and usdt @80 Rs/- per $ equating to 50$ in each pair
    '''
    global current_usdt_price
    global min_usdt_balance
    # request - 2
    current_usdt_price = private_api_exc.fetch_ticker('USDT/INR')['last']
    public_api_requests += 1
    inr_balance = current_balance['INR']
    usdt_balance = current_balance['USDT']
    inr_usdt_equvivalent = round(inr_balance / current_usdt_price, 1)
    print("opening balance-inr_balance, usdt_balance, inr_usdt_equvivalent, usdt_price", inr_balance, usdt_balance,
          inr_usdt_equvivalent, current_usdt_price)
    if usdt_balance >= min_usdt_balance and inr_usdt_equvivalent >= min_usdt_balance:
        print('#case-1- no change needed')
        return current_usdt_price

    elif inr_usdt_equvivalent < min_usdt_balance and usdt_balance >= min_usdt_balance + (
            min_usdt_balance - inr_usdt_equvivalent):
        usdt_to_sell = min_usdt_balance - inr_usdt_equvivalent
        if usdt_to_sell <1 and usdt_balance-min_usdt_balance>1:
            usdt_to_sell = math.ceil(usdt_to_sell)
        # sell usdt
        price_to_sell_at = get_price_for_required_quantity('sell', 'usdtinr', usdt_to_sell)
        if usdt_to_sell*price_to_sell_at <50 and inr_usdt_equvivalent <2.5:
            print("amount too less for order")
            return False
        print(private_api_exc.create_limit_sell_order('usdtinr', usdt_to_sell, price_to_sell_at))
        private_api_requests += 1
        print('#case-2- short in inr: & enough usdt balance', usdt_to_sell)
        return current_usdt_price

    elif usdt_balance < min_usdt_balance and inr_usdt_equvivalent >= min_usdt_balance + (
            min_usdt_balance - usdt_balance):
        usdt_to_buy = min_usdt_balance - usdt_balance
        price_to_buy_at = get_price_for_required_quantity('buy', 'usdtinr', usdt_to_buy)
        if usdt_to_buy*price_to_buy_at <50 and inr_usdt_equvivalent-min_usdt_balance>=1:
            usdt_to_buy = 1
        print(private_api_exc.create_limit_buy_order('usdtinr', usdt_to_buy, price_to_buy_at))
        private_api_requests += 1
        print('#case-3:short in usdt & enough inr balance', usdt_to_buy)
        return current_usdt_price
    else:
        print('#case-4: not enough usdt or inr balance', 'inr: ', inr_balance, 'inr_usdt_equvivalent: ',
              inr_usdt_equvivalent, 'usdt" ', usdt_balance)
        return False


def process_data(_d):
    _d['inr-buy-usdt-conversion'] = _d.get('inrbuy', 0) / usdt_price
    _d['inr-sell-usdt-conversion'] = _d.get('inrsell', 0) / usdt_price
    _d['sell_difference_in_$'] = _d['usdtsell'] - _d['inr-sell-usdt-conversion']
    _d['buy_differnce_in_$'] = _d['usdtbuy'] - _d['inr-buy-usdt-conversion']
    _d['min_market_sell_price_$'] = min(_d['inr-sell-usdt-conversion'], _d['usdtsell'])
    _d['final_buy_pair'] = 'inr' if _d['inr-sell-usdt-conversion'] < _d['usdtsell'] else 'usdt'
    _d['final_sell_pair'] = 'inr' if _d['inr-buy-usdt-conversion'] > _d['usdtbuy'] else 'usdt'
    _d['max_market_buy_price_$'] = max(_d['inr-buy-usdt-conversion'], _d['usdtbuy'])
    _d['profit'] = ((_d['max_market_buy_price_$'] - _d['min_market_sell_price_$']) * 100 / _d[
        'min_market_sell_price_$']) - 0.2
    _d['indexed_time'] = str(datetime.now(tz))
    return _d


@timeit
def get_balance_diff_after_trade():
    global private_api_requests
    current_balnce = private_api_exc.fetch_free_balance()
    private_api_requests += 1
    change_in_inr = primary_balance['INR'] - current_balnce['INR']
    change_in_usdt = primary_balance['USDT'] - current_balnce['USDT']
    print('current_usdt_price', current_usdt_price, 'inr change', change_in_inr, 'usdt change', change_in_usdt)


def get_ideal_profit(final_buy_pair, final_buy_price, final_sell_pair, final_sell_price, usdt_price):
    if final_buy_pair == final_sell_pair:
        profit = final_sell_price - final_buy_price
        print(f"profit in {final_buy_pair}, {profit}")
    elif final_buy_pair == 'usdt':
        inr_sell_usdt_eqv = final_sell_price / usdt_price
        profit = inr_sell_usdt_eqv - final_buy_price
        print(f"profit in {final_buy_price}, {profit}")
    else:
        final_buy_usdt_eqv = final_buy_price / usdt_price
        profit = final_sell_price - final_buy_usdt_eqv
        print(f"profit in usdt, {profit}")


try:
    usdt_price_if_enough_balance = maintain_min_inr_usdt_balance(primary_balance)
    time.sleep(1)
    if usdt_price_if_enough_balance:
        start = datetime.now()
        r = requests.get('https://api.wazirx.com/api/v2/market-status')
        end = datetime.now()
        print('time_taken', str(end - start)[:10])
        data_dict = enumerate(r.json().get('markets'))
        tz = pytz.timezone('UTC')
        d = []
        d1 = {}
        usdt_price = usdt_price_if_enough_balance
        coins_list = []
        for j, i in data_dict:
            if i['type'] == 'SPOT' and i['quoteMarket'] in ('inr', 'usdt') and i['status'] != 'suspended':
                if i.get('baseMarket') == 'usdt':
                    usdt_price = float(i['sell'])
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

        # buy_coin
        if sorted_coins_list:
            trading_coin = sorted_coins_list[0]
            print(sorted_coins_list[0])
            buy_coin_pair = trading_coin['coin'] + trading_coin['final_buy_pair']
            final_buy_pair = trading_coin['final_buy_pair']
            if final_buy_pair == 'usdt':
                buy_price = trading_coin['usdtsell'] * price_correction_rate
                quantity = round(min_usdt_balance / buy_price, 3)
            else:
                buy_price = trading_coin['inrsell'] * price_correction_rate
                quantity = round(min_usdt_balance * usdt_price / buy_price, 3)
            price = get_price_for_required_quantity('buy', buy_coin_pair, quantity)
            # if -0.002 < ((price - buy_price) / buy_price) < 0.002:
            print('*buy order placed, pair, quantiy, price *', buy_coin_pair, quantity, price)
            buy_order_id = private_api_exc.create_limit_buy_order(buy_coin_pair, quantity, price)['info']['id']
            final_buy_price = price * quantity
            print('buy order id', buy_order_id)
            private_api_requests += 1
            # funds_quantity = private_api_exc.fetch_free_balance()[trading_coin['coin'].upper()]
            # private_api_requests += 1
            # retries = 0
            # while funds_quantity < quantity or retries >= 10:
            #     print("sleeping", 1)
            #     time.sleep(1)
            #     funds_quantity = private_api_exc.fetch_free_balance()[trading_coin['coin'].upper()]
            #     private_api_requests += 1
            #     retries += 1
            # print("* buy order executed *")
            # if retries >= 5:
            #     private_api_exc.cancel_order(buy_order_id, buy_coin_pair)
            #     private_api_requests += 1
            time.sleep(0.2)
            sell_quantity = quantity
            final_sell_pair = trading_coin['final_sell_pair']
            if final_sell_pair == 'usdt':
                sell_price = trading_coin['usdtbuy'] * 0.998
            else:
                sell_price = trading_coin['inrbuy']
            sell_coin_pair = trading_coin['coin'] + trading_coin['final_sell_pair']
            s_price = get_price_for_required_quantity('sell', sell_coin_pair, sell_quantity)
            # if -0.001 < (s_price - sell_price) / sell_price > 0.001:
            print("sell order placed,sell_coin_pair, sell_quantity, s_price: ", sell_coin_pair, sell_quantity, s_price)
            private_api_exc.create_limit_sell_order(sell_coin_pair, sell_quantity, s_price)
            final_sell_price = sell_quantity * s_price
            private_api_requests += 1
            print("usdt_price", usdt_price)
            # final_balance = private_api_exc.fetch_free_balance()
            # private_api_requests += 1
            # print('primary_balance: INR: ', primary_balance['INR'], 'usdt: ', primary_balance['USDT'])
            # print('final_balance: INR: ', final_balance['INR'], 'usdt: ', final_balance['USDT'])
            # else:
            #     print("price fluctuated after buying,returning bought coins at available best price")
            #     private_api_exc.create_limit_sell_order(sell_coin_pair, sell_quantity, s_price)
            #     private_api_requests += 1
            # else:
            #     print("price fluctuates >0.1%,wont trade", price, buy_price)
            # get_balance_diff_after_trade()
            print("final_buy_pair,final_buy_price, final_sell_pair, final_sell_price, usdt_price", final_buy_pair,
                  final_buy_price, final_sell_pair, final_sell_price, usdt_price)
            get_ideal_profit(final_buy_pair, final_buy_price, final_sell_pair, final_sell_price, usdt_price)
        else:
            print("no coins with given profit percentage")

except Exception as e:
    print("exception", e)
    print('public_api_requests', public_api_requests, 'private_api_requests ', private_api_requests)
    traceback.print_exc()
# get_balance_diff_after_trade()

print("**done**")
