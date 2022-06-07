import pandas as pd
from pandas_datareader import data as web
from datetime import date,datetime
from dateutil.relativedelta import relativedelta
from sqlalchemy import create_engine
import psycopg2

import warnings

warnings.filterwarnings('ignore')

def crawler_stocks(stocks, owned, start, end,engine):
    #db_connection
    #postgresql+psycopg2://airflow:airflow@postgres/airflow
    
    #Variação total
    counter = 0

    for stock,stock_type in stocks.items():
        print(stock)
        if counter == 0:
            df = web.DataReader(stock, data_source = 'yahoo', start=start, end = end)
            df['stock'] = stock
            df['type'] = stock_type
            df.reset_index(inplace=True)
            counter = counter + 1
        else:
            df_append = web.DataReader(stock, data_source = 'yahoo', start=start, end = end)
            df_append['stock'] = stock
            df_append['type'] = stock_type
            df_append.reset_index(inplace=True)
            df = pd.concat([df,df_append])

        if stock not in owned:
            df.loc[df['stock'] == stock, 'owned'] = 'No'
        else:
            df.loc[df['stock'] == stock, 'owned'] = 'Yes'

    df.drop('Adj Close', axis=1, inplace=True)

    columns = ['High','Low','Open','Close']
    total_variation = df.copy()
    for col in columns:
        total_variation[col] = round(total_variation[col],2)
        total_variation[col] = total_variation[col].astype(str)
        total_variation[col]=total_variation[col].str.replace('.',',')
        #df[col] = df[col].astype(float)

    total_variation = total_variation.rename(columns={'Date':'date_ref','High':'max_value','Low':'min_value',
                                                    'Close':'close_value','Open':'open_value','Volume':'volume'})
    total_variation['date_ref'] = pd.to_datetime(total_variation['date_ref']).dt.date
    total_variation = total_variation[['stock','type','max_value','min_value','open_value','close_value','volume','owned','date_ref']]

    print('Salvando no banco...')
    total_variation.to_sql('stocks', engine, if_exists='replace',index=False)

    return df


def get_infos(df, stocks, engine):
    investiments = pd.read_excel("/opt/airflow/data/Controles_Ações_Fundos.xlsx", engine='openpyxl')
    big_n = pd.DataFrame({'stock':stocks.keys(),'type':stocks.values()})

    for stock,stock_type in stocks.items():
        print(stock)
        df_i = investiments.loc[(investiments['Papel'] == stock) &
                                (investiments['Preço_venda']).isna()]
        df_s = df.loc[df['stock'] == stock]
        big_n.loc[big_n['stock'] == stock, 'hist_min_value'] = min(df_s['Close'])
        big_n.loc[big_n['stock'] == stock, 'hist_max_value'] = max(df_s['Close'])
        big_n.loc[big_n['stock'] == stock, 'actual_value'] = df_s.Close.iloc[-1]
        big_n.loc[big_n['stock'] == stock, 'owned'] = df_s['owned']
        qt_owned = 0
        total_invested = 0
        
        for index, row in df_i.iterrows():
            qt_owned = qt_owned + row.Quantidade_Compra
            total_invested = total_invested + row.Valor_Compra_Total
            
        big_n.loc[big_n['stock'] == stock, 'qt_owned'] = qt_owned
        big_n.loc[big_n['stock'] == stock, 'total_invested'] = total_invested

    for index, row in big_n.iterrows():
        big_n.loc[index, 'actual_investiment'] = (row.actual_value * row.qt_owned)
        big_n.loc[index, 'gain_loss_value'] = (row.actual_value * row.qt_owned) - row.total_invested
        
    for index, row in big_n.iterrows():   
        if row.gain_loss_value < 0:
            big_n.loc[index, 'status'] = 'Loss'
        elif row.gain_loss_value == 0:
            big_n.loc[index, 'status'] = 'Even'
        else:
            big_n.loc[index, 'status'] = 'Gain'
            
    for col in big_n.select_dtypes(include='number').columns:
        big_n[col] = round(big_n[col],2)
        big_n[col] = big_n[col].astype(str)
        big_n[col]=big_n[col].str.replace('.',',')
        
    big_n = big_n[['stock','type','hist_min_value','hist_max_value','actual_value','qt_owned',
                'total_invested','actual_investiment','gain_loss_value','status','owned']]

    big_n.to_sql('infos', engine, if_exists='replace',index=False)


def calculate_variations(df,stocks,engine):
    index = 12
    counter = 0
    today = date.today()
    df_copy = df.copy()
    df_copy['Year'] = df_copy['Date'].dt.year
    df_copy['Month'] = df_copy['Date'].dt.month
    df_end = df_copy.loc[(df_copy['Year'] == today.year)]
    end = df_end.Date.iloc[-1]

    for value in range(1,index+1,1): 
        
        begin = end - relativedelta(months=value)
        
        df_begin = df_copy.loc[(df_copy['Year'] == begin.year) & 
                            (df_copy['Month'] == begin.month)]
        
        begin = df_begin.Date.iloc[0]
        n_months = (end.year - begin.year) * 12 + (end.month - begin.month)
        
        if n_months == 1:
            n_months = str(n_months) + ' month'
            print('Calculating variation for '+n_months+'. Initial date: ', begin, ' - final date: ', end)
        else:
            n_months = str(n_months) + ' meses'
            print('Calculating variation for '+n_months+'. Initial date: ', begin, ' - final date: ', end)
        
        for stock,stock_type in stocks.items():
            df_begin_stock = df_begin.loc[(df_begin['stock'] == stock) & 
                                        (df_begin['Date'] == begin)]
            
            df_end_Stock = df_end.loc[(df_end['stock'] == stock) & 
                                        (df_end['Date'] == end)]
            
            if len(df_begin_stock) > 0 and len(df_end_Stock) > 0: 
                print('Stock: ',stock, ' Final value: ', df_end_Stock.Close.iloc[0], ' - Initial value: ',df_begin_stock.Close.iloc[0])
                absolute_variation = round(df_end_Stock.Close.iloc[0]-df_begin_stock.Close.iloc[0],2)
                relative_variation = round((df_end_Stock.Close.iloc[0]/df_begin_stock.Close.iloc[0]-1),4)
                print('Variation: ', relative_variation*100)
            else:
                print('Stock: ',stock, ' Final value: ', 0, ' - Initial value: ',0)
                absolute_variation = round(0,2)
                relative_variation = round(0,4)
                print('variação: ', relative_variation*100)
        
            if counter == 0:
                dicti = {'period_ref':n_months,'date_ref':begin, 'relative_variation':float(relative_variation),
                        'absolute_variation':float(absolute_variation), 'stock':stock,'type':stock_type}
                series = pd.Series(dicti)
                df_relative = pd.DataFrame(series)
                df_relative = df_relative.T
                counter = counter+1
            else:
                dicti = {'period_ref':n_months,'date_ref':begin, 'relative_variation':float(relative_variation), 
                        'absolute_variation':float(absolute_variation),'stock':stock,'type':stock_type}
                df_relative = df_relative.append(dicti, ignore_index = True)
            
            

    begin = end - relativedelta(months=24)
    # Usamos apenas o ano aqui pois ao fazer o fim pegando o ultimo dia útil, o inicio (que é 2 anos atrás) pode cair em um dia que está fora do limite
    #dos dados. No exemplo de desenvolvimento, a data atual é 01-05, o fim é o último dia útil, que nesse caso é 29-04. Logo, se usassemos ano e mês
    #para o inicio, terimos 29-04-2020 e o nosso limite de dados está indo até 01-05-2020
    df_begin = df_copy.loc[(df_copy['Year'] == begin.year)] 

    begin = df_begin.Date.iloc[0]
    n_months = '24 meses'
    print('Calculating variation for '+n_months+'. Initial date: ', begin, ' - final date: ', end)

    for stock,stock_type in stocks.items():
        df_owned = df_copy.loc[df_copy['stock'] == stock]
        df_begin_stock = df_begin.loc[(df_begin['stock'] == stock) & 
                                    (df_begin['Date'] == begin)]

        df_end_stock = df_end.loc[(df_end['stock'] == stock) & 
                                    (df_end['Date'] == end)]
        if len(df_begin_stock) > 0 and len(df_end_stock) > 0:
            print('stock: ',stock, ' Valor Final: ', df_end_stock.Close.iloc[0], ' - Valor Inicial: ',df_begin_stock.Close.iloc[0])
            absolute_variation = round(df_end_stock.Close.iloc[0]-df_begin_stock.Close.iloc[0],2)
            relative_variation = round((df_end_stock.Close.iloc[0]/df_begin_stock.Close.iloc[0]-1),4)
            print('variação: ', relative_variation*100)
        else:
            print('stock: ',stock, ' Valor Final: ', 0, ' - Valor Inicial: ',0)
            absolute_variation = round(0,2)
            relative_variation = round(0,4)
            print('variação: ', relative_variation*100)
            
        dicti = {'period_ref':n_months,'date_ref':begin, 'relative_variation':float(relative_variation), 
                'absolute_variation':float(absolute_variation),'stock':stock,'type':stock_type}
        df_relative = df_relative.append(dicti, ignore_index = True)
        df_relative.loc[df_relative['stock'] == stock, 'owned'] = df_owned['owned']

    float_cols = ['relative_variation','absolute_variation']
    for col in float_cols:
        df_relative[col] = df_relative[col].astype(str)
        df_relative[col] = df_relative[col].str.replace('.',',')
        
    df_relative['date_ref'] = pd.to_datetime(df_relative['date_ref']).dt.date
    df_relative = df_relative[['stock','type','relative_variation','absolute_variation','period_ref','owned','date_ref']]

    df_relative.to_sql('variations', engine, if_exists='replace',index=False)


def get_dividends_data(stocks, owned, start, end, engine):
    counter = 0

    for stock,stock_type in stocks.items():
        if counter == 0:
            print(stock)
            dividends = web.DataReader(stock, 'yahoo-dividends', start = start, end = end)
            dividends['stock'] = stock
            dividends['stock_type'] = stock_type
            dividends.reset_index(inplace=True)
            counter = counter + 1
        else:
            print(stock)
            dividends_append = web.DataReader(stock, 'yahoo-dividends', start = start, end = end)
            dividends_append['stock'] = stock
            dividends_append['stock_type'] = stock_type
            dividends_append.reset_index(inplace=True)
            dividends = pd.concat([dividends,dividends_append])      

        if stock not in owned:
            dividends.loc[dividends['stock'] == stock, 'owned'] = 'No'
        else:
            dividends.loc[dividends['stock'] == stock, 'owned'] = 'Yes'
            
    dividends.drop('action', axis=1, inplace=True)     
    dividends = dividends.rename(columns={'index':'date_ref'})
    dividends['value'] = dividends['value'].astype(str)
    dividends['value']= dividends['value'].str.replace('.',',')
    dividends['date_ref'] = pd.to_datetime(dividends['date_ref']).dt.date
    dividends = dividends[['stock','stock_type','value','owned','date_ref']]

    dividends.to_sql('dividends', engine, if_exists='replace',index=False)


def main():
    conn_string = 'postgresql+psycopg2://postgres:Lara4ever!@host.docker.internal/bovespa_db'
    
    engine = create_engine(conn_string)
    #conn = db.connect()

    #Parametros
    today = datetime.today()
    two_years_before = today - relativedelta(years=2)
    today = today.strftime('%Y-%m-%d')
    two_years_before = two_years_before.strftime('%Y-%m-%d')

    print('data inicio: ', two_years_before)
    print('hoje: ', today)

    owned = ['ITSA4.SA','VBBR3.SA','BBDC4.SA','EGIE3.SA','ENBR3.SA','OIBR3.SA',
            'BCFF11.SA','HGLG11.SA','HTMX11.SA','KNRI11.SA','VISC11.SA',]

    stocks = {'ITSA4.SA':'Stock',
            'VBBR3.SA':'Stock',
            'BBDC4.SA':'Stock',
            'EGIE3.SA':'Stock',
            'ENBR3.SA':'Stock',
            'OIBR3.SA':'Stock',
    #-------------------------------------------------------
            'BCFF11.SA':'Investment Fund',
            'HGLG11.SA':'Investment Fund',
            'HTMX11.SA':'Investment Fund',
            'KNRI11.SA':'Investment Fund',
            'VISC11.SA':'Investment Fund',
    #--------------------------------------------------------
            'MGLU3.SA':'Stock',
            'COCA34.SA':'Stock',}

    data = crawler_stocks(stocks, owned, two_years_before, today,engine)
    get_infos(data, stocks, engine)
    calculate_variations(data,stocks,engine)
    get_dividends_data(stocks,owned,two_years_before,today,engine)
    


