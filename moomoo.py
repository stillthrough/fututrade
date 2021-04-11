import pandas as pd
import futu as ft
from futu import *
import datetime as dt 
import numpy as np
import getpass
import openpyxl
import json

def establish_connections(user_connection_type
                        , unlock_trade = True
                        , user_host = '127.0.0.1', user_port = 11111
                        , user_is_encrypt = None
                        , user_security_firm = SecurityFirm.FUTUINC):
    '''
    Ingest: Pass in connection criterias
    Output: Initialized instance/object of either trade/quote
    '''
    
    #Validate connection type
    connection_type_choices = ['trade', 'quote']
    assert user_connection_type in connection_type_choices, \
    f'connection type must be in {[choice for choice in connection_type_choices]}'

    
    try:
        #Intialized trading API
        if user_connection_type == 'trade':    
            trader = ft.OpenUSTradeContext(host = user_host, port = user_port
                                        , is_encrypt = user_is_encrypt
                                        , security_firm = user_security_firm)
    
            #If user wants to unlock account, ask for password to unlock
            if unlock_trade == True:
                user_pass = getpass.getpass()
                    #Log in first in order to pull account info
                trader.unlock_trade(password = user_pass, is_unlock = True)

            return trader

        #Initialize quote API
        elif user_connection_type == 'quote':
            quoter = ft.OpenQuoteContext(host = user_host, port = user_port
                                        , is_encrypt = user_is_encrypt)
            return quoter
        
    except Exception as e:
        raise e

def get_account_id(instance, account_type = 'live'
                    , acc_index = None):

    '''
    Ingest: Pass in initiallized instance of trade context
    Output: Accounts ids that fall under input account_type
    '''

    #Ensure account_type inputs are right
    account_type_dict = {'live': 'REAL'
                        ,'paper': 'SIMULATE'}
    assert account_type in account_type_dict.keys(), \
    f'account_type needs be in {(account for account in account_type_dict.keys())}'

    #Returning a tuple and [1] is the dataframe
    accounts_df = instance.get_acc_list()[1]

    #Get account ids matching account_type criteria
    account_id_outputs = []
    for row in accounts_df.itertuples():
        if row.trd_env == account_type_dict.get(account_type):
            account_id_outputs.append(row.acc_id)

    if len(account_id_outputs) > 1:
        return account_id_outputs[acc_index]
    elif len(account_id_outputs)  == 1:
        return account_id_outputs[0]



def get_account_balance(instance):

    #Log in first in order to pull account info
    instance.unlock_trade(password = getpass.getpass()
                                    , is_unlock = True)

    #Pull account info and store all numbers in a dictionary
    all_balance = instance.accinfo_query(trd_env = TrdEnv.REAL
                    , acc_id = get_account_id(instance), refresh_cache = False
                    , currency = ft.Currency.USD)[1]

    all_balance_dict = dict(zip(all_balance.columns, all_balance.values[0]))

    return all_balance_dict

def get_historical_trades (instance, trade_acc_id
                        , trade_period = 'today'
                        , trade_code = '', trade_start = '', trade_end = '' 
                        , trade_mode = TrdEnv.REAL
                        , acc_index = 0):

    #If no start date provided, default to a week ago to reduce data load
    if trade_period == 'today':
        trades_df = instance.deal_list_query(
                    code = trade_code, trd_env = trade_mode, acc_id = trade_acc_id
                    , acc_index = 0, refresh_cache = False)[1]

    
    elif trade_period == 'historical':    
        if not trade_start:
            trade_start = (dt.datetime.today() - dt.timedelta(days = 7)).strftime('%Y-%m-%d')

        #Assigning variable to df of trade transaction
        trades_df = instance.history_deal_list_query(
                            code = trade_code, start = trade_start, end = trade_end
                            , trd_env = trade_mode, acc_id = trade_acc_id
                            , acc_index = 0)[1]
    else:
        raise ValueError(f'{trade_period} is not an accpetable option')
    
    '''Format dataframe prior to exporting
    1) Get rid of unnecesary  columns and rename them for easier understanding
    '''    
    # 1.1 - Drop counter_broker information as not needed
    trades_df = trades_df.drop(columns = ['counter_broker_id', 'counter_broker_name'])
    # 1.2 - Assign new column names that are easier to understand
    new_col_names = ['ticker', 'name', 'deal_id', 'order_id', 'size', 'price'                         , 'direction','trade_time', 'trade_status']
    trades_df.columns = new_col_names


    #2.1 - Format columns into approriate formats
    trades_df = trades_df.astype({
                                'ticker': str, 'name': str
                                , 'deal_id': str, 'order_id': str
                                , 'size': float, 'price': float
                                , 'direction': str, 'trade_status': str
                                    })
    trades_df['trade_time'] = trades_df['trade_time'].values.astype('datetime64[s]')
                                
    # 3.1 - Add columns for future analytical needs
    trades_df['ticker'] = trades_df['ticker'].apply(lambda x: x.replace('US.',''))
    trades_df['amount'] = trades_df.apply(lambda x: 
                        -1 * (x['size'] * x['price']) if x['direction'] == 'BUY'
                        else x['size'] * x['price'], axis = 1)
    trades_df['trade_date'] = trades_df['trade_time'].dt.date              
    trades_df['trade_hour'] = trades_df['trade_time'].dt.hour.astype('int64') 
    trades_df['trade_minute'] = trades_df['trade_time'].dt.minute.astype('int64')

    trades_df = trades_df.sort_values(by = 'trade_time', ascending = True)

    return trades_df


def add_to_db (target_df, file_path = None
                , destination_sheet = 'Main'):
    '''
    Write or append trade history to db Excel workbook
    '''
    #Make sure the folder for the excel file existsa
    file_path = r"C:\Users\Weili\Desktop\FutuHistory\Futu_Transactions.xlsx"\
                                            if not file_path else file_path
    folder_name = os.path.dirname(file_path)
    

    if not os.path.exists(folder_name):
        os.mkdir(folder_name)

    try:
        #Ensure file currently exists
        if os.path.isfile(file_path):
            matrix_adj = {'row': 1, 'column': 1}
            current_matrix = pd.read_excel(file_path, sheet_name = destination_sheet
                                            , header = 0).shape
            #If file exist but no record, then start on first row and include header
            #If has records, then get max row number and +1 to get starting row
            if current_matrix[0] == 0:
                start_row, include_header = 0, True
            else:
                start_row = current_matrix[0] + matrix_adj.get('row')

            with pd.ExcelWriter(file_path, engine = 'openpyxl', mode = 'a') as writer:
                writer.sheets = {ws.title : ws for ws in writer.book.worksheets}
                target_df.to_excel(writer, sheet_name = destination_sheet
                                , index = False, header = False
                                , startrow = start_row)
        else:
            target_df.to_excel(file_path, index = False, sheet_name = destination_sheet)

    except (Exception, BaseException) as e:
        raise e

