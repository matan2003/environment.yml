import yfinance as yf
from datetime import datetime
import pandas as pd
import pandas_datareader as pdr

# sets specific hours for which the code will run
today = datetime.now().strftime('%Y-%m-%d %H-%M')
close_hour = datetime.now().strftime('%Y/%m/%d') + '-23:00'
# if datetime.now() == close_hour:

# list of the tickers
tickers_list = ['AMZN', 'AAPL', 'MSFT']

# Data Frames place holders
close_90m = pd.DataFrame()
open_90m = pd.DataFrame()
high_90m = pd.DataFrame()
low_90m = pd.DataFrame()
close_1d = pd.DataFrame()
open_1d = pd.DataFrame()
high_1d = pd.DataFrame()
low_1d = pd.DataFrame()
close_30m = pd.DataFrame()
open_30m = pd.DataFrame()
high_30m = pd.DataFrame()
low_30m = pd.DataFrame()

# import the stocks data
for ticker in tickers_list:
    stock_data_30m = yf.download(tickers=ticker, interval='30m', period='1d')
    stock_data_90m = yf.download(tickers=ticker, interval='90m', period='7d')

    close_30m[ticker] = stock_data_30m['Close']
    open_30m[ticker] = stock_data_30m['Open']
    high_30m[ticker] = stock_data_30m['High']
    low_30m[ticker] = stock_data_30m['Low']

    close_90m[ticker] = stock_data_90m['Close']
    open_90m[ticker] = stock_data_90m['Open']
    high_90m[ticker] = stock_data_90m['High']
    low_90m[ticker] = stock_data_90m['Low']

    close_1d[ticker] = stock_data_90m['Close'].resample(rule='D').last()
    open_1d[ticker] = stock_data_90m['Open'].resample(rule='D').first()
    high_1d[ticker] = stock_data_90m['High'].resample(rule='D').max()
    low_1d[ticker] = stock_data_90m['Low'].resample(rule='D').min()
# makes the data frame not include the aftermarket hours
close_30m = close_30m.shift(1)
open_30m = open_30m.shift(1)
high_30m = high_30m.shift(1)
low_30m = low_30m.shift(1)
close_90m = close_90m.shift(1)
open_90m = open_90m.shift(1)
high_90m = high_90m.shift(1)
low_90m = low_90m.shift(1)

# drop NaN values
open_90m.dropna(inplace=True)
close_90m.dropna(inplace=True)
high_90m.dropna(inplace=True)
low_90m.dropna(inplace=True)
open_30m.dropna(inplace=True)
close_30m.dropna(inplace=True)
high_30m.dropna(inplace=True)
low_30m.dropna(inplace=True)
open_1d.dropna(inplace=True)
close_1d.dropna(inplace=True)
high_1d.dropna(inplace=True)
low_1d.dropna(inplace=True)


# get specific price points by date
def price_point(data, ticker, days_back):
    if ticker is False:
        stock_data = data
    else:
        stock_data = data[ticker]
    length = len(stock_data)
    stock_data = stock_data.iloc[length - days_back - 1]
    return stock_data


# fibonacci levels calculation
def fibonacci(timeframe, ticker, percent):
    if percent == 1.23:
        look_back = 1
    else:
        look_back = 0

    # time frame calculation
    if timeframe == '1d':
        high = high_1d
        close = close_1d
        low = low_1d
        open = open_1d
    elif timeframe == '90m':
        high = high_90m
        close = close_90m
        low = low_90m
        open = open_90m
    elif timeframe == '30m':
        high = high_30m
        close = close_30m
        low = low_30m
        open = open_30m
    # fibonacci calculation
    calculate = (price_point(data=high,
                             ticker=ticker,
                             days_back=look_back) -
                 price_point(data=low,
                             ticker=ticker,
                             days_back=look_back)) * percent

    if price_point(data=close, ticker=ticker, days_back=look_back) > price_point(data=open, ticker=ticker,
                                                                                 days_back=look_back):
        final_calculation = (price_point(data=low, ticker=ticker, days_back=look_back)) + calculate
    else:
        final_calculation = (price_point(data=high, ticker=ticker, days_back=look_back)) - calculate
    return final_calculation


# Rate Of Change calculation
def rate_of_change(data_base, ticker, length, days_back):
    roc = ((price_point(data=data_base, ticker=ticker, days_back=days_back) -
            price_point(data=data_base, ticker=ticker, days_back=days_back + length)) /
           price_point(data=data_base, ticker=ticker, days_back=days_back + length)) * 100

    return round(roc, 4)


# rate of change moving average
def roc_sma(data_base, ticker, length, sma_length):
    days_back = 0
    sum = 0
    for days_back in range(sma_length):
        roc = ((price_point(data=data_base, ticker=ticker, days_back=days_back) -
                price_point(data=data_base, ticker=ticker, days_back=days_back + length)) /
               price_point(data=data_base, ticker=ticker, days_back=days_back + length)) * 100

        sum = roc + sum

        days_back += 1

    return round(sum / sma_length, 4)


def volume(time_frame):
    tickers = pd.read_html(
        'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')[0]

    # Get the data for the tickers from yahoo finance
    data_close = yf.download(tickers.Symbol.to_list(), interval=time_frame, period='3d', auto_adjust=True)['Close']
    data_volume = yf.download(tickers.Symbol.to_list(), interval=time_frame, period='3d', auto_adjust=True)['Volume']

    sum = 0
    previews_sum = 0

    for ticker in tickers.Symbol.to_list():

        try:

            # daily time frame
            if price_point(data=data_close, ticker=ticker, days_back=0) > price_point(data=data_close, ticker=ticker,
                                                                                      days_back=1):
                sum += price_point(data=data_volume, ticker=ticker, days_back=0) * price_point(data=data_close,
                                                                                                  ticker=ticker,
                                                                                                  days_back=0)

            else:
                sum -= price_point(data=data_volume, ticker=ticker, days_back=0) * price_point(data=data_close,
                                                                                                  ticker=ticker,
                                                                                                  days_back=0)

            # previews sum calculation
            if price_point(data=data_close, ticker=ticker, days_back=1) > price_point(data=data_close, ticker=ticker,
                                                                                      days_back=2):
                previews_sum += price_point(data=data_volume, ticker=ticker, days_back=1) * price_point(
                    data=data_close, ticker=ticker, days_back=1)

            else:
                previews_sum -= price_point(data=data_volume, ticker=ticker, days_back=1) * price_point(
                    data=data_close, ticker=ticker, days_back=1)
        except:
            pass

        # buy and sell signals
    if previews_sum > 0 and sum > previews_sum * 1.23:
        return "buy"
    elif previews_sum < 0 and sum < previews_sum * 1.23:
        return 'sell'
    else:
        return 'DONT TRADE'


# this indicator measures the stocks with positive relative/negative strength against the S&P500.
def relative_strength(time_frame):
    try:

        tickers = pd.read_html(
            'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')[0]
        progress = 0
        trend_strength = 0
        # imports the data
        spy_close = yf.download(tickers="SPY", interval=time_frame, period='6d', auto_adjust=True)['Close']
        data_close = yf.download(tickers.Symbol.to_list(), interval=time_frame, period='6d', auto_adjust=True)['Close']

        for ticker in tickers.Symbol.to_list():
            try:
                # calculation
                market_cap_data = pdr.get_quote_yahoo(ticker)['marketCap']
                print(market_cap_data)
                data = data_close[ticker] / spy_close
                progress += 1
                print(progress, "/500")
                # relative strength calculation
                roc_5 = rate_of_change(data_base=data, ticker=False, length=2, days_back=0)
                sma = roc_sma(data_base=data, ticker=False, length=2, sma_length=5)
                if roc_5 > sma:
                    trend_strength += market_cap_data[ticker]
                else:
                    trend_strength -= market_cap_data[ticker]
                print(trend_strength)
            except:
                pass
    except:
        pass

    return trend_strength


print(fibonacci(timeframe='90m', ticker='AAPL', percent=0.9))
