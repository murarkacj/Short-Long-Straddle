import logging
from datetime import datetime
import calendar
import pytz
import time
from kiteconnect import KiteConnect
import pandas as pd


def short_net_quant_zero(existing_positions,name):
    if len(existing_positions)==0 :
        return True
    else:
        p = True
        for i in existing_positions:
                if name in i['tradingsymbol'] and i['quantity'] < 0:
                    p = False
        return p

def long_net_quant_zero(existing_positions,name):
    if len(existing_positions)==0 :
        return True
    else:
        p = True
        for i in existing_positions:
                if name in i['tradingsymbol'] and i['quantity'] > 0:
                    p = False
        return p    
    
def short_get_symbol_lotsize(instruments,name,last_thursday_date_dt,kite,client):
    IST = pytz.timezone('Asia/Kolkata')
    logging.info(f'{datetime.now(IST)} For {client} Scanning Entry Short Straddle Option Chain of {name}')
    ltp = kite.ltp(f'NSE:{name}')[f'NSE:{name}']['last_price']
    strike = None  # Initialize ATM to None
    diff = None
    tradingsymbol_ce=None
    lot_size_ce=None
    tradingsymbol_pe=None
    lot_size_pe = None
    for i in instruments:
        if i['instrument_type']=='CE':
            if i['name'] == name:
                if i['expiry'] == last_thursday_date_dt:
                        if strike is None or abs(float(i['strike']) - ltp) < diff:
                            strike = i['strike']
                            diff = abs(float(strike - ltp))
                            tradingsymbol_ce = i['tradingsymbol']
                            lot_size_ce = i['lot_size']
                            instru_ce = i['instrument_token']
    ce_ltp = kite.ltp(f'NFO:{tradingsymbol_ce}')[f'NFO:{tradingsymbol_ce}']['last_price']
    pe_ltp = None
    diff = None
    token_ltp = []
    for j in instruments:
        if j['name'] == name:
            if j['expiry'] == last_thursday_date_dt:
                if j['instrument_type']=='PE':
                    token_ltp.append('NFO:'+j['tradingsymbol'])
    token_ltp = tuple(token_ltp)
    ltp_prices = kite.ltp(token_ltp)
    for j in instruments:
        if j['name'] == name:
            if j['expiry'] == last_thursday_date_dt:
                if j['instrument_type']=='PE':
                    ltp_data = ltp_prices['NFO:'+j['tradingsymbol']]
                    if ltp_data:
                        price = ltp_data['last_price']
                        if price != 0:
                            if pe_ltp is None or abs(float(price) - ce_ltp) < diff:
                                pe_ltp = price
                                diff = abs(float(price - ce_ltp))
                                tradingsymbol_pe = j['tradingsymbol']
                                lot_size_pe = j['lot_size']   
                                instru_pe = j['instrument_token']
    return tradingsymbol_ce,lot_size_ce,tradingsymbol_pe,lot_size_pe,instru_ce,instru_pe

def long_get_symbol_lotsize(existing_positions,instruments,name,last_thursday_date_dt,kite):
    
    ce_rover = None
    pe_rover = None  
    for position in existing_positions:
        if name in str(position['tradingsymbol']) and 'CE' in str(position['tradingsymbol'])[-2:] and position['quantity']<0:
            ce_rover = position
        if name in str(position['tradingsymbol']) and 'PE' in str(position['tradingsymbol'])[-2:] and position['quantity']<0:
            pe_rover = position
    
    if ce_rover and pe_rover:
        ce_row = ce_rover
        short_instru_ce,short_tradsym_ce =  ce_row['instrument_token'],ce_row['tradingsymbol']  
        pe_row = pe_rover
        short_instru_pe,short_tradsym_pe =  pe_row['instrument_token'],pe_row['tradingsymbol']

        for i in instruments:
            if i['tradingsymbol'] == short_tradsym_ce:
                ce_strike = i['strike']
            if i['tradingsymbol'] == short_tradsym_pe:
                pe_strike = i['strike']

        long_ce_strike = None
        long_pe_strike = None

        diff = None        
        for i in instruments:
            if i['instrument_type']=='CE':
                if i['name'] == name:
                    if i['expiry'] == last_thursday_date_dt:
                        if i['strike']<ce_strike:
                            if long_ce_strike is None or abs(float(i['strike']) - ce_strike) < diff:
                                long_ce_strike = i['strike']
                                diff = abs(float(long_ce_strike - ce_strike))
                                tradingsymbol_ce = i['tradingsymbol']
                                lot_size_ce = i['lot_size']
                                instru_ce = i['instrument_token']    
        diff = None  
        for i in instruments:
            if i['instrument_type']=='PE':
                if i['name'] == name:
                    if i['expiry'] == last_thursday_date_dt:
                        if i['strike']>pe_strike:
                            if long_pe_strike is None or abs(float(i['strike']) - pe_strike) < diff:
                                long_pe_strike = i['strike']
                                diff = abs(float(long_pe_strike - pe_strike))
                                tradingsymbol_pe = i['tradingsymbol']
                                lot_size_pe = i['lot_size']
                                instru_pe = i['instrument_token']  

        return tradingsymbol_ce,lot_size_ce,tradingsymbol_pe,lot_size_pe,instru_ce,instru_pe
    else:
        return None,None,None,None,None,None

def place_order(kite,tradingSymbol, price, qty, direction, exchangeType, product, orderType):
    try:
        orderId = kite.place_order(
            variety=kite.VARIETY_REGULAR,
            exchange=exchangeType,
            tradingsymbol=tradingSymbol,
            transaction_type=direction,
            quantity=qty,
            price=price,
            product=product,
            order_type=orderType)

        logging.info('Order placed successfully, orderId = %s', orderId)
        return orderId
    except Exception as e:
        logging.info('Order placement failed: %s', e.message)

def get_name_from_instrument_token(instruments,instrument_token):
    for instrument in instruments:
        if int(instrument['instrument_token']) == int(instrument_token):
            return instrument['name']
    return None

def short_get_instru_tradesymbol_pe_from_ce(existing_positions,name):
    for position in existing_positions:
        if name in position['tradingsymbol'] and 'PE' in position['tradingsymbol'][-2:] and position['quantity'] < 0:
            return position['instrument_token'],position['tradingsymbol']
        
def long_get_instru_tradesymbol_pe_from_ce(existing_positions,name):
    for position in existing_positions:
        if name in position['tradingsymbol'] and 'PE' in position['tradingsymbol'][-2:] and position['quantity'] > 0:
            return position['instrument_token'],position['tradingsymbol']
        
def get_sell_ce(kite,name):
    IST = pytz.timezone('Asia/Kolkata')
    null = None
    false = False
    time.sleep(0.3)
    orders = kite.orders()
    diff = None
    last_time = None
    for i in orders:
        if ((i['exchange_update_timestamp'] is not None) 
            and name in i['tradingsymbol']  
            and 'CE' in i['tradingsymbol'][-2:] 
            and i['transaction_type'] == 'SELL'
            and i['status']=='COMPLETE'):
            datetime_object = i['exchange_update_timestamp']
            time_difference = datetime.now(IST) - datetime_object
            if last_time is None or time_difference < diff:
                last_time = datetime_object
                diff = time_difference
                price = i['average_price']
    return price
                
def get_sell_pe_from_ce(kite,name):
    IST = pytz.timezone('Asia/Kolkata')
    null = None
    false = False
    time.sleep(0.3)
    orders = kite.orders()
    diff = None
    last_time = None
    for i in orders:
        if ((i['exchange_update_timestamp'] is not None) 
            and name in i['tradingsymbol']  
            and 'PE' in i['tradingsymbol'][-2:] 
            and i['transaction_type'] == 'SELL'
            and i['status']=='COMPLETE'):
            datetime_object = i['exchange_update_timestamp']
            time_difference = datetime.now(IST) - datetime_object
            if last_time is None or time_difference < diff:
                last_time = datetime_object
                diff = time_difference
                price = i['average_price']
    return price
    
  
def cal_dates():
    IST = pytz.timezone('Asia/Kolkata')
    # Calculate the dae of last friday and thursday of the current month
    year = int(datetime.now(IST).today().strftime('%Y'))
    month = int(datetime.now(IST).today().strftime('%m'))
    last_day = calendar.monthrange(year, month)[1]
    last_weekday = calendar.weekday(year, month, last_day)
    last_thursday = last_day - ((7 - (3 - last_weekday)) % 7)
    last_thursday_date = datetime(year, month, last_thursday).strftime('%d-%m-%Y')
    last_friday = last_day - ((7 - (4 - last_weekday)) % 7)
    last_thursday_date_dt = datetime.strptime(last_thursday_date,'%d-%m-%Y').date()
    first_weekday = calendar.weekday(year,month,1)
    days_to_add = (4-first_weekday+7)%7
    first_friday = 1 + days_to_add
    return first_friday,last_friday,last_thursday_date_dt

def cal_sec_last_thurs():
    # Calculate the dae of last friday and thursday of the current month
    IST = pytz.timezone('Asia/Kolkata')
    year = int(datetime.now(IST).today().strftime('%Y'))
    month = int(datetime.now(IST).today().strftime('%m'))
    last_day = calendar.monthrange(year, month)[1]
    last_weekday = calendar.weekday(year, month, last_day)
    last_thursday = last_day - ((7 - (3 - last_weekday)) % 7)
    second_last_thurs = last_thursday - 7
    return second_last_thurs

def short_straddle(client,name,val,kite,instruments,existing_positions):
    IST = pytz.timezone('Asia/Kolkata')
    first_friday,last_friday,last_thursday_date_dt = cal_dates()
    # Check if it's time to enter the trade
    if (
        datetime.now(IST).time() >= datetime.strptime('09:30', '%H:%M').time()
        and (
            (int(datetime.now(IST).today().strftime('%d')) >= int(first_friday) and int(datetime.now(IST).today().strftime('%d')) < last_friday)
        or 
        (int(datetime.now(IST).today().strftime('%d')) == last_friday and datetime.now(IST).time() <= datetime.strptime('14:00', '%H:%M').time())
        )
        ):
        if short_net_quant_zero(existing_positions,name):
            tradingsymbol_ce,lot_size_ce,tradingsymbol_pe,lot_size_pe ,instru_ce,instru_pe = short_get_symbol_lotsize(instruments,name,last_thursday_date_dt,kite,client)
            if (tradingsymbol_ce is not None and lot_size_ce is not None and tradingsymbol_pe is not None and lot_size_pe is not None):
                logging.info(f'{datetime.now(IST)} For {client} ENTERING SHORT STRADDLE FOR {val} lots\n{tradingsymbol_ce} OF LOT SIZE {lot_size_ce} \nand\n{tradingsymbol_pe} of LOT SIZE {lot_size_pe}')
                # place_order(kite,tradingsymbol_ce, 0, lot_size_ce*val, kite.TRANSACTION_TYPE_SELL, KiteConnect.EXCHANGE_NFO, KiteConnect.PRODUCT_NRML,
                # KiteConnect.ORDER_TYPE_MARKET)
                # place_order(kite,tradingsymbol_pe, 0, lot_size_pe*val, kite.TRANSACTION_TYPE_SELL, KiteConnect.EXCHANGE_NFO, KiteConnect.PRODUCT_NRML,
                #             KiteConnect.ORDER_TYPE_MARKET)

    # Check if it's time to exit the trade
    if datetime.now(IST).time() >= datetime.strptime('09:25', '%H:%M').time():
        for position in existing_positions:
            if (get_name_from_instrument_token(instruments,position['instrument_token']) == name 
                and position['quantity'] < 0 
                and 'CE' in position['tradingsymbol'][-2:]):  # Assuming short positions
                instru_ce = position['instrument_token']
                instru_pe,trad_pe = short_get_instru_tradesymbol_pe_from_ce(existing_positions,name)
                sell_ce = get_sell_ce(kite,position['tradingsymbol'])
                sell_pe = get_sell_pe_from_ce(kite,trad_pe)
                ltp_ce = ((kite.quote(int(instru_ce)))[str(instru_ce)])['last_price']
                ltp_pe = ((kite.quote(int(instru_pe)))[str(instru_pe)])['last_price']
                logging.info(f"{datetime.now(IST)} For {client} Checking Short Exit Condtion for {name} with current CE ltp {ltp_ce} & PE ltp {ltp_pe}")
                if (
                    (ltp_ce >= 2 * ltp_pe) or (ltp_pe >= 2 * ltp_ce)
                or (
                    int(datetime.now(IST).today().strftime('%d')) == last_friday
                    and datetime.now(IST).time() >= datetime.strptime('14:00', '%H:%M').time()
                )
                or
                (
                    (ltp_ce <= sell_ce*0.5) or (ltp_pe <= sell_pe*0.5)
                )
                ):
                    print(f'\nCode to Exit the Trade {name} ltp ce {ltp_ce} ,ltp pe {ltp_pe}')
                    # place_order(kite,position['tradingsymbol'], 0, position['quantity'], kite.TRANSACTION_TYPE_BUY, KiteConnect.EXCHANGE_NFO, KiteConnect.PRODUCT_NRML,
                    #             KiteConnect.ORDER_TYPE_MARKET)
                    # place_order(kite,trad_pe, 0, position['quantity'], kite.TRANSACTION_TYPE_BUY, KiteConnect.EXCHANGE_NFO, KiteConnect.PRODUCT_NRML,
                    #             KiteConnect.ORDER_TYPE_MARKET)
                else:
                    print(f'\n Exit Condtion not met for {name}, ltp ce {ltp_ce} ,ltp pe {ltp_pe}')
                break


def long_straddle(client,name,val,kite,instruments,existing_positions):
    IST = pytz.timezone('Asia/Kolkata')
    first_friday,last_friday,last_thursday_date_dt = cal_dates()
    second_last_thursday = cal_sec_last_thurs()
    # Check if it's time to enter the trade
    if (
        datetime.now(IST).time() >= datetime.strptime('15:25', '%H:%M').time()
        and (
            (int(datetime.now(IST).today().strftime('%d')) >= int(first_friday) and int(datetime.now(IST).today().strftime('%d')) <= second_last_thursday)
        )
        ):
        if long_net_quant_zero(existing_positions,name):
            tradingsymbol_ce,lot_size_ce,tradingsymbol_pe,lot_size_pe ,instru_ce,instru_pe = long_get_symbol_lotsize(existing_positions,instruments,name,last_thursday_date_dt,kite)
            if (tradingsymbol_ce is not None and lot_size_ce is not None and tradingsymbol_pe is not None and lot_size_pe is not None):
                print(f'\nFor {client} ENTERING Long STRADDLE FOR {val} lots\n{tradingsymbol_ce} OF LOT SIZE {lot_size_ce} \nand\n{tradingsymbol_pe} of LOT SIZE {lot_size_pe}')
                # place_order(kite,tradingsymbol_ce, 0, lot_size_ce*val, kite.TRANSACTION_TYPE_BUY, KiteConnect.EXCHANGE_NFO, KiteConnect.PRODUCT_NRML,
                # KiteConnect.ORDER_TYPE_MARKET)
                # place_order(kite,tradingsymbol_pe, 0, lot_size_pe*val, kite.TRANSACTION_TYPE_BUY, KiteConnect.EXCHANGE_NFO, KiteConnect.PRODUCT_NRML,
                #             KiteConnect.ORDER_TYPE_MARKET)

    # Check if it's time to exit the trade
    if (datetime.now(IST).time() >= datetime.strptime('09:25', '%H:%M').time()
        and datetime.now(IST).time() <= datetime.strptime('09:30', '%H:%M').time()
    ):
        for position in existing_positions:
            if (get_name_from_instrument_token(instruments,position['instrument_token']) == name 
                and position['quantity'] > 0 
                and 'CE' in position['tradingsymbol'][-2:]):  # Assuming short positions
                instru_ce = position['instrument_token']
                instru_pe,trad_pe = long_get_instru_tradesymbol_pe_from_ce(existing_positions,name)
                ltp_ce = ((kite.quote(int(instru_ce)))[str(instru_ce)])['last_price']
                ltp_pe = ((kite.quote(int(instru_pe)))[str(instru_pe)])['last_price']
                print(f'\nCode to Exit the Trade {name} ltp ce {ltp_ce} ,ltp pe {ltp_pe}')
                # place_order(kite,position['tradingsymbol'], 0, position['quantity'], kite.TRANSACTION_TYPE_SELL, KiteConnect.EXCHANGE_NFO, KiteConnect.PRODUCT_NRML,
                #             KiteConnect.ORDER_TYPE_MARKET)
                # place_order(kite,trad_pe, 0, position['quantity'], kite.TRANSACTION_TYPE_SELL, KiteConnect.EXCHANGE_NFO, KiteConnect.PRODUCT_NRML,
                #             KiteConnect.ORDER_TYPE_MARKET)
            else:
                print(f'\n Exit Condtion not met for {name}, ltp ce {ltp_ce} ,ltp pe {ltp_pe}')
            break