**ITCH-5.0-Reader**

Hello there, this is a parser for NASDAQ ITCH 5.0 data.
It will parse the data and store the data in three dictionaries:
1. _premarket_trade_: It will store the total trade value(price*vol) of a stock in premarket hours
2. _premarket_volume_: It will store the total volume of a stock in premarket hours
3. _livemarket_trade_: It will store the total trade value(price*vol) of a stock in live market hours
4. _livemarket_volume_: It will store the total volume of a stock in live market hours
5. _postmarket_trade_: It will store the total trade value(price*vol) of a stock in postmarket hours
6. _postmarket_volume_: It will store the total volume of a stock in postmarket hours


**The output structure will be like:**
    
for premarket_VWAP and postmarket_VWAP:
- file will be named as premarket(or postmarket)_trade_{hour_from_midnight}.txt
- inside which every line is of the form: {starthour:00:00 , endhour:00:00, stock, VWAP}
- Here, starthour is current hour from midnight and endhour is next hour from midnight (basically trading hour window)
    
for livemarket_VWAP:
- file will be named as livemarket_trade_{hour_from_opening}.txt
- inside which every line is of the form: {starthour:00:00 , endhour:00:00, stock, VWAP}
- Here, starthour is current hour from opening and endhour is next hour from opening (basically trading hour window)
- now for the close, VWAP will be calculated from 3:30 PM to 4:00 PM
- close VWAP will be in the file named as livemarket_trade_close.txt

**Data download link:**
https://emi.nasdaq.com/ITCH/Nasdaq%20ITCH/01302019.NASDAQ_ITCH50.gz