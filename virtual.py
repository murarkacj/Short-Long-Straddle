import logging
from datetime import datetime
import calendar
import sqlite3
import pytz
import time
from kiteconnect import KiteConnect
import pandas as pd
  
IST = pytz.timezone('Asia/Kolkata')

          
def short_net_quant_zero(instruments,name):
    try:
        # Connect to the SQLite database or create it if not exists
        sqliteConnection = sqlite3.connect('SQLite_Python.db')

        # Create a cursor to interact with the database
        cursor = sqliteConnection.cursor()
        # print("Database created and Successfully Connected to SQLite")

        # Fetch all rows from the 'portfolio' table
        fetch_all_query = '''
            SELECT * FROM portfolio;
        '''
        cursor.execute(fetch_all_query)
        rows = cursor.fetchall()

        # Print fetched rows
        # print("Fetched Rows from 'portfolio' table:")
        if len(rows)==0:
            # Close the cursor
            cursor.close()
            return True
        elif len(rows)>0:
            p = True
            for position in rows:
                posi_quan = 0
                if (get_name_from_instrument_token(instruments,position[2]) == name):  # Assuming short positions
                    if position[1] < 0:
                        for posi in rows:
                            if posi[0] == position[0]:
                                posi_quan += posi[1]
                if posi_quan < 0:
                        p = False
            return p
            
        # Close the cursor
        cursor.close()

    except sqlite3.Error as error:
        print("Error while working with SQLite:", error)
    finally:
        # Close the database connection if it's open
        if sqliteConnection:
            sqliteConnection.close()
            # print("The SQLite connection is closed")
            
def long_net_quant_zero(instruments,name):
    try:
        # Connect to the SQLite database or create it if not exists
        sqliteConnection = sqlite3.connect('SQLite_Python.db')

        # Create a cursor to interact with the database
        cursor = sqliteConnection.cursor()
        # print("Database created and Successfully Connected to SQLite")

        # Fetch all rows from the 'portfolio' table
        fetch_all_query = '''
            SELECT * FROM portfolio;
        '''
        cursor.execute(fetch_all_query)
        rows = cursor.fetchall()

        # Print fetched rows
        # print("Fetched Rows from 'portfolio' table:")
        if len(rows)==0:
            # Close the cursor
            cursor.close()
            return True
        elif len(rows)>0:
            p = True
            for position in rows:
                posi_quan = 0
                if (get_name_from_instrument_token(instruments,position[2]) == name):  # Assuming short positions
                    if position[1] > 0:
                        for posi in rows:
                            if posi[0] == position[0]:
                                posi_quan += posi[1]
                if posi_quan > 0:
                        p = False
            return p
            
        # Close the cursor
        cursor.close()

    except sqlite3.Error as error:
        print("Error while working with SQLite:", error)
    finally:
        # Close the database connection if it's open
        if sqliteConnection:
            sqliteConnection.close()
            # print("The SQLite connection is closed")

def short_get_symbol_lotsize(instruments,name,last_thursday_date_dt,kite):
    logging.info(f'Scanning Entry Short Straddle Option Chain for {name}')
    IST = pytz.timezone('Asia/Kolkata')
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

def long_get_symbol_lotsize(rows,instruments,name,last_thursday_date_dt,kite):
            
    for row in rows:
        if name in str(row[0]) and 'CE' in str(row[0])[-2:] and row[1]<0:
            ce_rover = row
        if name in str(row[0]) and 'PE' in str(row[0])[-2:] and row[1]<0:
            pe_rover = row
    
    ce_row = ce_rover
    short_instru_ce,short_tradsym_ce =  ce_row[2],ce_row[0]  
    pe_row = pe_rover
    short_instru_pe,short_tradsym_pe =  pe_row[2],pe_row[0]
    
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

def get_instru_tradesymbol_pe_from_ce(rows,name):
    for row in rows:
        if name in str(row[0]) and 'PE' in str(row[0])[-2:]:
            posi_quan = 0
            for rw in rows:
                if rw[0] == row[0]:
                    posi_quan += rw[1]
            if posi_quan!=0:
                rover = row       
    row = rover
    return row[2],row[0]    

def get_sell_pe_from_ce(existing_positions,name):
    for position in existing_positions:
        if (name in position[0] and 'PE' in (position[0])[-2:]):
            rover = position
    position = rover
    return position[3]
        
def cal_dates():
    # Calculate the dae of last friday and thursday of the current month
    IST = pytz.timezone('Asia/Kolkata')
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
        if short_net_quant_zero(instruments,name):
            tradingsymbol_ce,lot_size_ce,tradingsymbol_pe,lot_size_pe ,instru_ce,instru_pe = short_get_symbol_lotsize(instruments,name,last_thursday_date_dt,kite)
            if (tradingsymbol_ce is not None and lot_size_ce is not None and tradingsymbol_pe is not None and lot_size_pe is not None):
                logging.info(f'{datetime.now(IST)} ENTERING SHORT STRADDLE FOR \n{tradingsymbol_ce} OF LOT SIZE {lot_size_ce} & {val} lots\nand\n{tradingsymbol_pe} of LOT SIZE {lot_size_pe} & {val} lots')

                ltp_ce = ((kite.quote(int(instru_ce)))[str(instru_ce)])['last_price']
                ltp_pe = ((kite.quote(int(instru_pe)))[str(instru_pe)])['last_price']

                try:
                    sqliteConnection = sqlite3.connect('SQLite_Python.db')
                    cursor = sqliteConnection.cursor()
                    # print("Database created and Successfully Connected to SQLite")
                    
                    insert_data_query = '''
                        INSERT INTO portfolio (tradingsymbol, quantity, instrument_token,sell_price,timestamp)
                        VALUES (?, ?, ?,?,?);
                    '''
                    data_to_insert = (tradingsymbol_ce, lot_size_ce*-1*val,instru_ce,ltp_ce,datetime.now(IST))
                    cursor.execute(insert_data_query, data_to_insert)
                    data_to_insert = (tradingsymbol_pe, lot_size_pe*-1*val,instru_pe,ltp_pe,datetime.now(IST))
                    cursor.execute(insert_data_query, data_to_insert)

                    sqliteConnection.commit()
                    # print("Row of data inserted into 'portfolio' table")
                    # Close the cursor
                    cursor.close()
                except sqlite3.Error as error:
                    print("Error while working with SQLite:", error)
                finally:
                    # Close the database connection if it's open
                    if sqliteConnection:
                        sqliteConnection.close()

    # Check if it's time to exit the trade
    if datetime.now(IST).time() >= datetime.strptime('09:25', '%H:%M').time():
        # Fetching all entries from table
        try:
            # Connect to the SQLite database or create it if not exists
            sqliteConnection = sqlite3.connect('SQLite_Python.db')

            # Create a cursor to interact with the database
            cursor = sqliteConnection.cursor()
            # print("Database created and Successfully Connected to SQLite")

            # Fetch all rows from the 'portfolio' table
            fetch_all_query = '''
                SELECT * FROM portfolio;
            '''
            cursor.execute(fetch_all_query)
            rows = cursor.fetchall()
            
            quan = 0
        
            for position in rows:
                if (get_name_from_instrument_token(instruments,position[2]) == name):  # Assuming short positions
                    quan += position[1]
            if quan < 0:
                for position in rows:
                    posi_quan = 0
                    if (get_name_from_instrument_token(instruments,position[2]) == name):  # Assuming short positions
                        if position[1] < 0:
                            for posi in rows:
                                if posi[0] == position[0]:
                                    posi_quan += posi[1]
                                    if posi_quan < 0:
                                        if 'CE' in position[0][-2:]:
                                            p = position
                position = p
                instru_ce = position[2]
                instru_pe,trad_pe = get_instru_tradesymbol_pe_from_ce(rows,name)
                for lol in rows:
                    if lol[0]==position[0]:
                        sell_ce = lol[3]
                sell_pe = get_sell_pe_from_ce(rows,name)
                ltp_ce = ((kite.quote(int(instru_ce)))[str(instru_ce)])['last_price']
                ltp_pe = ((kite.quote(int(instru_pe)))[str(instru_pe)])['last_price']
                logging.info(f"{datetime.now(IST)} For {client} Checking Short Exit Condtion for {name} with current CE ltp {ltp_ce} & PE ltp {ltp_pe}")
                if (
                    (ltp_ce >= 2 * ltp_pe) or (ltp_pe >= 2 * ltp_ce)
                or (
                    int(datetime.now(IST).today().strftime('%d')) == last_friday  # Check if it's a Friday
                    and datetime.now(IST).time() >= datetime.strptime('14:00', '%H:%M').time()
                )
                or
                (
                    (ltp_ce <= sell_ce*0.5) or (ltp_pe <= sell_pe*0.5)
                )):
                    try:
                        logging.info(f'{datetime.now(IST)} Exiting SHORT STRADDLE FOR \n{position[0]} Of Quantity {position[1]} \nand\n{trad_pe} of Quantity {position[1]}')

                        insert_data_query = '''
                            INSERT INTO portfolio (tradingsymbol, quantity, instrument_token,sell_price,timestamp)
                            VALUES (?, ?, ?,?,?);
                        '''
                        data_to_insert = (position[0], position[1]*-1,instru_ce,ltp_ce,datetime.now(IST))
                        cursor.execute(insert_data_query, data_to_insert)
                        data_to_insert = (trad_pe, position[1]*-1,instru_pe,ltp_pe,datetime.now(IST))
                        cursor.execute(insert_data_query, data_to_insert)

                        sqliteConnection.commit()
                        # print("Row of data inserted into 'portfolio' table")
                        # Close the cursor
                        # if sqliteConnection:
                        #     cursor.close()
                    except sqlite3.Error as error:
                        print("Error while working with SQLite:", error)

                    
            if sqliteConnection:    
                cursor.close()

        except sqlite3.Error as error:
            print("Error while working with SQLite:", error)
        finally:
            # Close the database connection if it's open
            if sqliteConnection:
                sqliteConnection.close()
                # print("The SQLite connection is closed")
                    
def long_straddle(client,name,val,kite,instruments,existing_positions):
    IST = pytz.timezone('Asia/Kolkata')
    first_friday,last_friday,last_thursday_date_dt = cal_dates()
    second_last_thursday = cal_sec_last_thurs()
    if (
        datetime.now(IST).time() >= datetime.strptime('15:25', '%H:%M').time()
        and (
            (int(datetime.now(IST).today().strftime('%d')) >= int(first_friday) and int(datetime.now(IST).today().strftime('%d')) <= second_last_thursday)
        )
        ):
        try:
            # Connect to the SQLite database or create it if not exists
            sqliteConnection = sqlite3.connect('SQLite_Python.db')

            # Create a cursor to interact with the database
            cursor = sqliteConnection.cursor()
            # print("Database created and Successfully Connected to SQLite")

            # Fetch all rows from the 'portfolio' table
            fetch_all_query = '''
                SELECT * FROM portfolio;
            '''
            cursor.execute(fetch_all_query)
            rows = cursor.fetchall()
            if sqliteConnection:    
                cursor.close()

        except sqlite3.Error as error:
            print("Error while working with SQLite:", error)
        finally:
            # Close the database connection if it's open
            if sqliteConnection:
                sqliteConnection.close()
                # print("The SQLite connection is closed")
                
        if long_net_quant_zero(instruments,name):
            tradingsymbol_ce,lot_size_ce,tradingsymbol_pe,lot_size_pe ,instru_ce,instru_pe = long_get_symbol_lotsize(rows,instruments,name,last_thursday_date_dt,kite)
            if (tradingsymbol_ce is not None and lot_size_ce is not None and tradingsymbol_pe is not None and lot_size_pe is not None):
                logging.info(f'{datetime.now(IST)} ENTERING Long STRADDLE FOR \n{tradingsymbol_ce} OF LOT SIZE {lot_size_ce} & {val} lots\nand\n{tradingsymbol_pe} of LOT SIZE {lot_size_pe} & {val} lots')

                ltp_ce = ((kite.quote(int(instru_ce)))[str(instru_ce)])['last_price']
                ltp_pe = ((kite.quote(int(instru_pe)))[str(instru_pe)])['last_price']

                try:
                    sqliteConnection = sqlite3.connect('SQLite_Python.db')
                    cursor = sqliteConnection.cursor()
                    # print("Database created and Successfully Connected to SQLite")
                    
                    insert_data_query = '''
                        INSERT INTO portfolio (tradingsymbol, quantity, instrument_token,sell_price,timestamp)
                        VALUES (?, ?, ?,?,?);
                    '''
                    data_to_insert = (tradingsymbol_ce, lot_size_ce*1*val,instru_ce,ltp_ce,datetime.now(IST))
                    cursor.execute(insert_data_query, data_to_insert)
                    data_to_insert = (tradingsymbol_pe, lot_size_pe*1*val,instru_pe,ltp_pe,datetime.now(IST))
                    cursor.execute(insert_data_query, data_to_insert)

                    sqliteConnection.commit()
                    # print("Row of data inserted into 'portfolio' table")
                    # Close the cursor
                    if sqliteConnection:
                        cursor.close()
                except sqlite3.Error as error:
                    print("Error while working with SQLite:", error)
                finally:
                    # Close the database connection if it's open
                    if sqliteConnection:
                        sqliteConnection.close()
    


    # Check if it's time to exit the trade
    if ((datetime.now(IST).time() >= datetime.strptime('09:25', '%H:%M').time()) 
        and (datetime.now(IST).time() < datetime.strptime('09:30', '%H:%M').time())
        ):
        # Fetching all entries from table
        try:
            # Connect to the SQLite database or create it if not exists
            sqliteConnection = sqlite3.connect('SQLite_Python.db')

            # Create a cursor to interact with the database
            cursor = sqliteConnection.cursor()
            # print("Database created and Successfully Connected to SQLite")

            # Fetch all rows from the 'portfolio' table
            fetch_all_query = '''
                SELECT * FROM portfolio;
            '''
            cursor.execute(fetch_all_query)
            rows = cursor.fetchall()
            ce_rover = None
            pe_rover = None
            if not long_net_quant_zero(instruments,name):
                for row in rows:
                    if name in str(row[0]) and 'CE' in str(row[0])[-2:] and row[1]>0:
                        ce_rover = row
                    if name in str(row[0]) and 'PE' in str(row[0])[-2:] and row[1]>0:
                        pe_rover = row
                if ce_rover and pe_rover:
                    ce_row = ce_rover
                    long_instru_ce,long_tradsym_ce,long_quan_ce,long_buy_ce =  ce_row[2],ce_row[0] ,ce_row[1],ce_row[3]
                    pe_row = pe_rover
                    long_instru_pe,long_tradsym_pe,long_quan_pe,long_buy_pe =  pe_row[2],pe_row[0],pe_row[1],pe_row[3]

                    ltp_ce = ((kite.quote(int(long_instru_ce)))[str(long_instru_ce)])['last_price']
                    ltp_pe = ((kite.quote(int(long_instru_pe)))[str(long_instru_pe)])['last_price']
                    try:
                        sqliteConnection = sqlite3.connect('SQLite_Python.db')
                        cursor = sqliteConnection.cursor()
                        # print("Database created and Successfully Connected to SQLite")
                        
                        
                        logging.info(f'{datetime.now(IST)} Exiting Long STRADDLE FOR \n{long_tradsym_ce} Of Quantity {long_quan_ce} \nand\n{long_tradsym_pe} of Quantity {long_quan_pe}')

                        insert_data_query = '''
                            INSERT INTO portfolio (tradingsymbol, quantity, instrument_token,sell_price,timestamp)
                            VALUES (?, ?, ?,?,?);
                        '''
                        data_to_insert = (long_tradsym_ce, long_quan_ce*-1,long_instru_ce,ltp_ce,datetime.now(IST))
                        cursor.execute(insert_data_query, data_to_insert)
                        data_to_insert = (long_tradsym_pe, long_quan_pe*-1,long_instru_pe,ltp_pe,datetime.now(IST))
                        cursor.execute(insert_data_query, data_to_insert)

                        sqliteConnection.commit()
                                            
                        if sqliteConnection:    
                            cursor.close()

                    except sqlite3.Error as error:
                        print("Error while working with SQLite:", error)
                    
            if sqliteConnection:    
                cursor.close()

        except sqlite3.Error as error:
            print("Error while working with SQLite:", error)
        finally:
            # Close the database connection if it's open
            if sqliteConnection:
                sqliteConnection.close()
                # print("The SQLite connection is closed")
