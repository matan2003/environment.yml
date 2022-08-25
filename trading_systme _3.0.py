import yfinance as yf
from datetime import datetime
from datetime import timedelta
import pandas as pd
import numpy as np
from polygon import RESTClient

# sets specific hours for which the code will run
today = datetime.now()

close_hour = datetime.now().strftime('%Y/%m/%d') + '-23:00'
# if datetime.now() == close_hour:



# list of the tickers
sp500_vol_1m = pd.DataFrame()
sp500_close_1m = pd.DataFrame()
sp500_open_1m = pd.DataFrame()
sp500_high_1m = pd.DataFrame()
sp500_low_1m = pd.DataFrame()
sp500_vol_30m = pd.DataFrame()
sp500_close_30m = pd.DataFrame()
sp500_open_30m = pd.DataFrame()
sp500_high_30m = pd.DataFrame()
sp500_low_30m = pd.DataFrame()
sp500_vol_1d = pd.DataFrame()
sp500_close_1d = pd.DataFrame()
sp500_open_1d = pd.DataFrame()
sp500_high_1d = pd.DataFrame()
sp500_low_1d = pd.DataFrame()
close_1m = pd.DataFrame()
open_1m = pd.DataFrame()
high_1m = pd.DataFrame()
low_1m = pd.DataFrame()
volume_1m = pd.DataFrame()

# import the stocks data
days_back = (today - timedelta(days=10)).strftime("%Y-%m-%d")
today = today.strftime("%Y-%m-%d")

tickers_list = ['SPY', 'AMZN', 'AAPL', 'MSFT']
client = RESTClient("wbcDXT5brKLSp_LdfDM2RyzEiLqFjrSg")

for ticker in tickers_list:
    temp_1m = client.get_aggs(ticker, 1, "minute", days_back, today, limit=50000)
    df = pd.DataFrame(temp_1m)
    df['date'] = pd.to_datetime(df['timestamp'], unit='ms')
    df = df.set_index('date')
    df = df.drop("timestamp", axis=1)
    close_1m[ticker] = df['close']
    open_1m[ticker] = df['open']
    high_1m[ticker] = df['high']
    low_1m[ticker] = df['low']
    volume_1m[ticker] = df['transactions']


# ohlcv_1d = yf.download(tickers=tickers_list, interval='1d', period='7d')

close_1d = close_1m.resample('1d').last()
open_1d = open_1m.resample('1d').first()
high_1d = high_1m.resample('1d').max()
low_1d = low_1m.resample('1d').min()
volume_1d = volume_1m.resample('1d').sum()

close_30m = close_1m.resample('30T').last()
open_30m = open_1m.resample('30T').first()
high_30m = high_1m.resample('30T').max()
low_30m = low_1m.resample('30T').min()
volume_30m = volume_1m.resample('30T').sum()
#
# close_1m.dropna(inplace=True)
# open_1m.dropna(inplace=True)
# volume_1m.dropna(inplace=True)
# high_1m.dropna(inplace=True)
# low_1m.dropna(inplace=True)
# close_30m.dropna(inplace=True)
# open_30m.dropna(inplace=True)
# high_30m.dropna(inplace=True)
# low_30m.dropna(inplace=True)
# volume_30m.dropna(inplace=True)
# close_1d.dropna(inplace=True)
# open_1d.dropna(inplace=True)
# high_1d.dropna(inplace=True)
# low_1d.dropna(inplace=True)
# volume_1d.dropna(inplace=True)

# close_1m = close_1m.shift(1)
# open_1m = open_1m.shift(1)
# high_1m = high_1m.shift(1)
# low_1m = low_1m.shift(1)
# volume_1m = volume_1m.shift(1)
# close_30m = close_30m.shift(1)
# open_30m = open_30m.shift(1)
# high_30m = high_30m.shift(1)
# low_30m = low_30m.shift(1)
# volume_30m = volume_30m.shift(1)

sp500_tickers = pd.read_html(
    'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')[0]
prog = 0
close_1d['high/low']=0
for ticker in sp500_tickers.Symbol.to_list():
    try:
        temp_1m = client.get_aggs(ticker, 1, "minute", days_back, today, limit=50000)

        df = pd.DataFrame(temp_1m)
        df['date'] = pd.to_datetime(df['timestamp'], unit='ms')
        temp_1m = df.set_index('date')

        sp500_vol_1m[ticker] = temp_1m['transactions']
        sp500_close_1m[ticker] = temp_1m['close']
        sp500_open_1m[ticker] = temp_1m['open']
        sp500_low_1m[ticker] = temp_1m['low']
        sp500_high_1m[ticker] = temp_1m['high']

        sp500_vol_30m[ticker] = temp_1m['transactions'].resample('30T').sum()
        sp500_close_30m[ticker] = temp_1m['close'].resample('30T').last()
        sp500_open_30m[ticker] = temp_1m['open'].resample('30T').first()
        sp500_low_30m[ticker] = temp_1m['low'].resample('30T').min()
        sp500_high_30m[ticker] = temp_1m['high'].resample('30T').max()

        sp500_vol_1d[ticker] = temp_1m['transactions'].resample('1d').sum()
        sp500_close_1d[ticker] = temp_1m['close'].resample('1d').last()
        sp500_open_1d[ticker] = temp_1m['open'].resample('1d').first()
        sp500_low_1d[ticker] = temp_1m['low'].resample('1d').min()
        sp500_high_1d[ticker] = temp_1m['high'].resample('1d').max()

        # sp500_close_1m.dropna(subset=[ticker], inplace=True)
        # sp500_close_1d.dropna(subset=[ticker], inplace=True)
        # sp500_close_30m.dropna(subset=[ticker], inplace=True)
        # sp500_vol_1m.dropna(subset=[ticker], inplace=True)
        # sp500_vol_1d.dropna(subset=[ticker], inplace=True)
        # sp500_close_30m.dropna(subset=[ticker], inplace=True)
        # sp500_open_1m.dropna(subset=[ticker], inplace=True)
        # sp500_open_1d.dropna(subset=[ticker], inplace=True)
        # sp500_open_30m.dropna(subset=[ticker], inplace=True)
        # sp500_low_1m.dropna(subset=[ticker], inplace=True)
        # sp500_low_1d.dropna(subset=[ticker], inplace=True)
        # sp500_low_30m.dropna(subset=[ticker], inplace=True)
        # sp500_high_1m.dropna(subset=[ticker], inplace=True)
        # sp500_high_30m.dropna(subset=[ticker], inplace=True)
        # sp500_high_1d.dropna(subset=[ticker], inplace=True)

        #
        # sp500_close_1m['Volume_time_price'] = sp500_vol_1m[ticker] * (
        #         (sp500_close_1m[ticker] + sp500_open_1m[ticker]) / 2)
        # sp500_close_1d['Volume_time_price'] = sp500_close_1m['Volume_time_price'].resample("1d").sum()
        # #sp500_close_1d.dropna(subset=['Volume_time_price'], inplace=True)
        # print(sp500_close_1d['Volume_time_price'])
        #
        # close_1d['high/low'] += np.where(sp500_close_1d[ticker] > (sp500_high_1d[ticker].shift(1)),
        #                                  sp500_close_1d['Volume_time_price'], (sp500_close_1d['Volume_time_price'] * -1))
        # print(close_1d['high/low'])


        prog += 1
        print("progress:", prog, '/500')
    except:
        pass


# high_low['new low'] += np.where(sp500_close_30m[ticker] < sp500_low_30m[ticker].shift(1),
#                           1, 0)
# print(close_30m['new high'] - close_30m['new low'])

# prog += 1
# print("importing data:", prog, "/500")


# makes the data frame not include the aftermarket hours
# sp500_close_1m = sp500_vol_1m.shift(1)
# sp500_vol_1m = sp500_close_1m.shift(1)
# sp500_high_1m = sp500_high_1m.shift(1)
# sp500_low_1m = sp500_low_1m.shift(1)
# sp500_open_1m = sp500_open_1m.shift(1)
# sp500_close_30m = sp500_vol_30m.shift(1)
# sp500_vol_30m = sp500_close_30m.shift(1)
# sp500_high_30m = sp500_high_30m.shift(1)
# sp500_low_30m = sp500_low_30m.shift(1)
# sp500_open_30m = sp500_open_30m.shift(1)


def fibonacci(timeframe, ticker, percent):
    if timeframe == '1d':
        low = low_1d
        high = high_1d
        open = open_1d
        close = close_1d
    elif timeframe == '30m':
        low = low_30m
        high = high_30m
        open = open_30m
        close = close_30m
    elif timeframe == '1m':
        low = low_1m
        high = high_1m
        open = open_1m
        close = close_1m
    close['calculation Positive'] = low[ticker] + (
            high[ticker] - low[ticker]) * percent
    close['calculation Negative'] = high[ticker] - (
            high[ticker] - low[ticker]) * percent
    close['Fibonacci_levels'] = np.where(close[ticker] > open[ticker],
                                         close['calculation Positive'],
                                         close['calculation Negative'])

    return close['Fibonacci_levels']


def roc(timeframe, ticker, period, sma_length):
    if timeframe == '1d':
        data_base = close_1d
    elif timeframe == '30m':
        data_base = close_30m
    elif timeframe == '1m':
        data_base = close_1m
    data_base['ROC'] = (data_base[ticker] - data_base[ticker].shift(period)) / data_base[
        ticker].shift(period) * 100
    data_base['ROC MA'] = data_base['ROC'].rolling(window=sma_length).mean()

    return data_base["ROC MA"]


def money_flow(timeframe):
    vol = sp500_vol_1m
    close = sp500_close_1m
    open = sp500_open_1m
    prog = 0
    close['Total Volume'] = 0
    for ticker in sp500_tickers.Symbol.to_list():
        try:
            close['Volume_time_price'] = vol[ticker] * ((close[ticker] + open[ticker]) / 2)
            close['Volume_time_price'].fillna(value=0, inplace=True)
            close['positive Volume'] = np.where(close[ticker] > close[ticker].shift(1), close['Volume_time_price'], 0)
            close['negative Volume'] = np.where(close[ticker] < close[ticker].shift(1), close['Volume_time_price'], 0)
            close['Total Volume'] += close['positive Volume'] - close['negative Volume']

            prog += 1
        except:
            pass

    if timeframe == '30m':
        df_30m = close['Total Volume'].resample("30T").sum()
        df_30m.drop(df_30m[df_30m == 0].index, inplace=True)
        return df_30m


    elif timeframe == '1d':
        df_1d = close['Total Volume'].resample("1d").sum()
        df_1d.drop(df_1d[df_1d == 0].index, inplace=True)
        return df_1d



    else:
        return close['Total Volume']


# def net_volume(timeframe):
#     if timeframe == '1m':
#         vol = sp500_vol_1m
#         close = sp500_close_1m
#         open = sp500_open_1m
#     elif timeframe == '30m':
#         vol = sp500_vol_30m
#         close = sp500_close_30m
#         open = sp500_open_30m
#     elif timeframe == '1d':
#         vol = sp500_vol_1d
#         close = sp500_close_1d
#         open = sp500_open_1d
#
#     vol['Total Volume'] = 0
#
#     for ticker in sp500_tickers.Symbol.to_list():
#         try:
#             vol['Volume_time_price'] = vol[ticker] * ((close[ticker] * 2 + open[ticker]) / 3)
#             vol['Volume_time_price'].dropna(inplace=True)
#             vol['Total Volume'] += vol['Volume_time_price']
#         except:
#             pass
#
#     return vol['Total Volume']

def net_volume():
    sp500_vol_1d['Total Volume'] = 0

    for ticker in sp500_tickers.Symbol.to_list():
        try:
            sp500_vol_1m['Volume_time_price'] = sp500_vol_1m[ticker] * (
                        (sp500_close_1m[ticker] + sp500_open_1m[ticker]) / 2)
            sp500_vol_1d['Volume_time_price'] = sp500_vol_1m['Volume_time_price'].resample('1d').sum()
            sp500_vol_1d.dropna(subset=['Volume_time_price'], inplace=True)
            sp500_vol_1d['Total Volume'] += sp500_vol_1d['Volume_time_price']
        except:
            pass
    sp500_vol_1d['Total Volume'] = sp500_vol_1d['Total Volume'][sp500_vol_1d['Total Volume'] != 0]
    sp500_vol_1d.dropna(subset=['Total Volume'], inplace=True)
    return sp500_vol_1d['Total Volume']


def new_high_low():
    close_1d['high/low'] = 0

    for ticker in sp500_tickers.Symbol.to_list():

        sp500_close_1m['Volume_time_price'] = sp500_vol_1m[ticker] * (
                (sp500_close_1m[ticker] + sp500_open_1m[ticker]) / 2)
        sp500_close_1d['Volume_time_price'] = sp500_close_1m['Volume_time_price'].resample("1d").sum()
        sp500_close_1d.dropna(subset=['Volume_time_price'], inplace=True)

        close_1d['high/low'] += np.where(sp500_close_1d[ticker] > sp500_high_1d[ticker].shift(1),
                                         sp500_close_1d['Volume_time_price'], sp500_close_1d['Volume_time_price'] * -1)

        # sp500_close_1d['lower lows'] = np.where(sp500_close_1d[ticker] < sp500_low_1d[ticker].shift(1),
        #                                         sp500_close_1d['Volume_time_price'], 0)
        #
        # close_1d['new low/high'] += sp500_close_1d['higher highs'] - sp500_close_1d['lower lows']
    close_1d['high/low'] = close_1d['high/low'][close_1d['high/low'] != 0]
    close_1d.dropna(subset=['high/low'], inplace=True)
    return close_1d['high/low']


def relative_strength(timeframe, period):
    if timeframe == '1m':
        sp500_close = sp500_close_1m
        sp500_vol = sp500_vol_1m
        close = close_1m
    elif timeframe == '30m':
        sp500_close = sp500_close_30m
        sp500_vol = sp500_vol_30m
        close = close_30m
    elif timeframe == '1d':
        sp500_close = sp500_close_1d
        sp500_vol = sp500_vol_1d
        close = close_1d

    close['TREND'] = 0
    for ticker in sp500_tickers.Symbol.to_list():
        stock_ratio = sp500_close[ticker] / close['SPY']
        close['Volume Price'] = sp500_close[ticker] * sp500_vol[ticker]
        stock_ratio.dropna(inplace=True)
        close.dropna(subset=['Volume Price'], inplace=True)
        close['ROC'] = ((stock_ratio - stock_ratio.shift(period)) / stock_ratio.shift(period)) * 100
        close['ROC MA'] = close['ROC'].rolling(window=period).mean()
        close['Volume Price'] = close['Volume Price'].rolling(window=period).mean()
        close['TREND'] += np.where(close['ROC'] > close['ROC MA'], close['Volume Price'],
                                   close['Volume Price'] * -1)

    close['TREND'] = close['TREND'][close['TREND'] != 0]
    return close['TREND']


print(new_high_low())


