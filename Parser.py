'''
Hello there, this is a parser for NASDAQ ITCH 5.0 data.
It will parse the data and store the data in three dictionaries:
    1. [premarket_trade: It will store the total trade value(price*vol) of a stock in premarket hours
        premarket_volume: It will store the total volume of a stock in premarket hours]
    2. [livemarket_trade: It will store the total trade value(price*vol) of a stock in live market hours
        livemarket_volume: It will store the total volume of a stock in live market hours]
    3. [postmarket_trade: It will store the total trade value(price*vol) of a stock in postmarket hours
        postmarket_volume: It will store the total volume of a stock in postmarket hours]
    
    The output structure will be like:
    for premarket_VWAP and postmarket_VWAP:
        =>file will be named as premarket(or postmarket)_trade_{hour_from_midnight}.txt
            - inside which every line is of the form: {starthour:00:00 , endhour:00:00, stock, VWAP}
            - Here, starthour is current hour from midnight and endhour is next hour from midnight (basically trading hour window)
    for livemarket_VWAP:
        =>file will be named as livemarket_trade_{hour_from_opening}.txt
            - inside which every line is of the form: {starthour:00:00 , endhour:00:00, stock, VWAP}
            - Here, starthour is current hour from opening and endhour is next hour from opening (basically trading hour window)
            - now for the close, VWAP will be calculated from 3:30 PM to 4:00 PM
            - close VWAP will be in the file named as livemarket_trade_close.txt
              
    I have avoided function calls in here, so that it can run faster. As there will be lesser function calls.
'''

import gzip
import time
import os

fileaddress = input("Enter the file address of gzip: ") # e.g. fileaddress = '01302019.NASDAQ_ITCH50.gz'
includedarkpool = input("Include darkpool? (Y/N) (any faulty value will take by default Y): ") != "N"
wantprint = input("Want to print the output data in txt files(around 10-20)? (Y/N) (default Y): ") != "N"
start_time = time.time()

# Initialization of necessary variables
premarket_trade = dict()
premarket_volume = dict()
livemarket_trade = dict()
livemarket_volume= dict()
postmarket_trade = dict()
postmarket_volume = dict()
preopen_market = False
live_market = False
postopen_market = False

file = gzip.open(fileaddress, 'rb') # Reading gzip file
next_data = file.read(1) # Reading first byte

while next_data: # Looping through the file till next_data is not empty
    curr_data = next_data
    if curr_data == b'S': # System Event Message
        message = file.read(11)
        timestamp = int.from_bytes(message[4:10], byteorder='big')
        print("S",timestamp/(6*1e10), message[10:11].decode('ascii'))
        if message[10:11].decode('ascii') == 'S': # Indicating start of System Hours
            preopen_market, live_market, postopen_market = True, False, False
        elif message[10:11].decode('ascii') == 'Q': # Indicating start of Market Hours
            preopen_market, live_market, postopen_market = False, True, False
        elif message[10:11].decode('ascii') == 'M': # Indicating End of Market Hours
            preopen_market, live_market, postopen_market = False, False, True
        else: # Indicating End of System Hours and Pre System Hours
            preopen_market, live_market, postopen_market = False, False, False

    elif curr_data == b'R': # Stock Directory Message (generally sent in Pre-System Hours)
        message = file.read(38)

    elif curr_data == b'H': # Stock Trading Action Message (generally sent in Pre-System Hours)
        message = file.read(24)

    elif curr_data == b'Y': # Reg SHO Short Sale Price Test Restricted Indicator Message
        message = file.read(19)

    elif curr_data == b'L': # Market Participant Position Message
        message = file.read(25)

    elif curr_data == b'V': # MWCB Decline Level Message
        message = file.read(34)

    elif curr_data == b'W': # Market Wide Circuit Breaker status Message
        message = file.read(11)

    elif curr_data == b'K': # IPO Quoting Period Update Message
        message = file.read(27)
        
    elif curr_data == b'J': # LULD Auction Collar Message
        message = file.read(34)
        
    elif curr_data == b'h': # Operational Halt Message
        message = file.read(20)

    elif curr_data == b'A': # Add Order Message - no MPID Attribution
        message = file.read(35)

    elif curr_data == b'F': # Add Order Message - MPID Attribution
        message = file.read(39)

    elif curr_data == b'E': # Order Executed Message
        message = file.read(30)

    elif curr_data == b'C': # Order Executed With Price Message
        message = file.read(35)

    elif curr_data == b'X': # Order Cancel Message
        message = file.read(22)

    elif curr_data == b'D': # Order Delete Message
        message = file.read(18)

    elif curr_data == b'U': # Order Replace Message
        message = file.read(34)
    
    elif curr_data == b'P': # Trade Message Non-Cross
        message = file.read(43)
        timestamp = int.from_bytes(message[4:10], byteorder='big')
        shares = int.from_bytes(message[19:23], byteorder='big')
        stock = message[23:31].decode('ascii').strip()
        price = int.from_bytes(message[31:35], byteorder='big')/1e4
        current_hour = int((timestamp//1e11)//36)
        if preopen_market:
            if premarket_trade.get(current_hour) == None:
                premarket_trade[current_hour] = {stock: shares*price}
                premarket_volume[current_hour] = {stock: shares}
            else:
                if premarket_trade[current_hour].get(stock) == None:
                    premarket_trade[current_hour][stock] = shares*price
                    premarket_volume[current_hour][stock] = shares
                else:
                    premarket_trade[current_hour][stock] += shares*price
                    premarket_volume[current_hour][stock] += shares
        
        elif live_market:
            current_hour = int(timestamp/(36*1e11) - 9.5) # i.e. opening 9:30 AM
            if livemarket_trade.get(current_hour) == None:
                livemarket_trade[current_hour] = {stock: shares*price}
                livemarket_volume[current_hour] = {stock: shares}
            else:
                if livemarket_trade[current_hour].get(stock) == None:
                    livemarket_trade[current_hour][stock] = shares*price
                    livemarket_volume[current_hour][stock] = shares
                else:
                    livemarket_trade[current_hour][stock] += shares*price
                    livemarket_volume[current_hour][stock] += shares
        
        elif postopen_market:
            if postmarket_trade.get(current_hour) == None:
                postmarket_trade[current_hour] = {stock: shares*price}
                postmarket_volume[current_hour] = {stock: shares}
            else:
                if postmarket_trade[current_hour].get(stock) == None:
                    postmarket_trade[current_hour][stock] = shares*price
                    postmarket_volume[current_hour][stock] = shares
                else:
                    postmarket_trade[current_hour][stock] += shares*price
                    postmarket_volume[current_hour][stock] += shares
    
    elif curr_data == b'Q': # Trade Message Cross (buyer and seller are same generally darkpools)
        message = file.read(39)
        if includedarkpool:
            timestamp = int.from_bytes(message[4:10], byteorder='big')
            shares = int.from_bytes(message[10:18], byteorder='big')
            stock = message[18:26].decode('ascii').strip()
            price = int.from_bytes(message[26:30], byteorder='big')/1e4
            current_hour = int((timestamp//1e11)//36)
            if preopen_market:
                if premarket_trade.get(current_hour) == None:
                    premarket_trade[current_hour] = {stock: shares*price}
                    premarket_volume[current_hour] = {stock: shares}
                else:
                    if premarket_trade[current_hour].get(stock) == None:
                        premarket_trade[current_hour][stock] = shares*price
                        premarket_volume[current_hour][stock] = shares
                    else:
                        premarket_trade[current_hour][stock] += shares*price
                        premarket_volume[current_hour][stock] += shares
        
            elif live_market:
                current_hour = int(timestamp/(36*1e11) - 9.5) # i.e. opening 9:30 AM
                if livemarket_trade.get(current_hour) == None:
                    livemarket_trade[current_hour] = {stock: shares*price}
                    livemarket_volume[current_hour] = {stock: shares}
                else:
                    if livemarket_trade[current_hour].get(stock) == None:
                        livemarket_trade[current_hour][stock] = shares*price
                        livemarket_volume[current_hour][stock] = shares
                    else:
                        livemarket_trade[current_hour][stock] += shares*price
                        livemarket_volume[current_hour][stock] += shares
            
            elif postopen_market:
                if postmarket_trade.get(current_hour) == None:
                    postmarket_trade[current_hour] = {stock: shares*price}
                    postmarket_volume[current_hour] = {stock: shares}
                else:
                    if postmarket_trade[current_hour].get(stock) == None:
                        postmarket_trade[current_hour][stock] = shares*price
                        postmarket_volume[current_hour][stock] = shares
                    else:
                        postmarket_trade[current_hour][stock] += shares*price
                        postmarket_volume[current_hour][stock] += shares
                    

    elif curr_data == b'B': # Broken Trade Message
        message = file.read(18)

    elif curr_data == b'I': # Net Order Imbalance Indicator Message
        message = file.read(49)
    
    elif curr_data == b'N': # Retail Interest Message
        message = file.read(19)
    
    next_data = file.read(1)

print("time taken: ", time.time() - start_time , "seconds")

if wantprint:
    # check if we have parsed_data folder or not
    # if not, create one
    if not os.path.exists('./parsed_data'):
        os.makedirs('./parsed_data')

    # printing of premarket_VWAP
    for hour in premarket_trade:
        with open(f'./parsed_data/premarket_trade_{hour}.txt', 'w') as f:
            for stock in premarket_trade[hour]:
                premarket_trade[hour][stock] /= premarket_volume[hour][stock]
                f.write(f"{hour}:00:00 , {hour+1}:00:00, {stock}, {premarket_trade[hour][stock]}\n")
    
    # printing of livemarket_VWAP
    for hour in livemarket_trade:
        if hour == 6:
            with open('./parsed_data/livemarket_trade_close.txt','w') as f:
                for stock in livemarket_trade[hour]:
                    if livemarket_volume[hour][stock] == 0:
                        livemarket_trade[hour][stock] = 0
                        continue
                    livemarket_trade[hour][stock] /= livemarket_volume[hour][stock]
                    f.write(f"{hour+9}:30:00 , {hour+10}:00:00, {stock}, {livemarket_trade[hour][stock]}\n")
        else:
            with open(f'./parsed_data/livemarket_trade_{hour}.txt', 'w') as f:
                for stock in livemarket_trade[hour]:
                    if livemarket_volume[hour][stock] == 0:
                        livemarket_trade[hour][stock] = 0
                        continue
                    livemarket_trade[hour][stock] /= livemarket_volume[hour][stock]
                    f.write(f"{hour+9}:30:00 , {hour+10}:30:00, {stock}, {livemarket_trade[hour][stock]}\n")
    
    # printing of postmarket_VWAP
    for hour in postmarket_trade:
        with open(f'./parsed_data/postmarket_trade_{hour}.txt', 'w') as f:
            for stock in postmarket_trade[hour]:
                if postmarket_volume[hour][stock] == 0:
                    postmarket_trade[hour][stock] = 0
                    continue
                postmarket_trade[hour][stock] /= postmarket_volume[hour][stock]
                # f.write( hour:00:00 stock price\n)
                f.write(f"{hour}:00:00 , {hour+1}:00:00, {stock}, {postmarket_trade[hour][stock]}\n")