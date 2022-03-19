import pandas as pd
import requests
import json
from datetime import datetime
import mysql.connector
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
root_path = "https://www.hkex.com.hk"
def row_to_df(body:dict)->pd.DataFrame:
    df_dict = {}
    for item in body:
        if item['row'] not in df_dict.keys():
            df_dict[item['row']] = []
        if item['isNumField'] == True:
            item['text'] = int(item['text'].replace(',',''))
        df_dict[item['row']].append(item['text'])
    return pd.DataFrame(df_dict.values())

def column_to_list(column) ->list:
    column_dict=[]
    for item in column:
        column_dict.append(item['text'])
    return column_dict

if __name__ == '__main__':

    data = requests.get(root_path+'/eng/stat/dmstat/marksum/DailyStatistics_F1_HSI_290.json')
    try:
        data_table =data.json()['tables']
    except Exception as e:
        print(e)
        print(data.json())
    dataset = data_table[0]['body']
    df = row_to_df(dataset)
    df.columns = ['Date','C1', 'C2', 'Volume', 'OI',]
    df['Date']=pd.to_datetime(df['Date'],format='%Y %m %d')
    db_cred = json.load(open('config/db.json'))
    conn = mysql.connector.connect(**db_cred)
    cursor = conn.cursor(buffered=True)
    data_query = 'INSERT IGNORE INTO future_oi_hsi (trading_date,current_price,forward_price,current_volume,open_interest) ' \
                 'values (%(Date)s,%(C1)s,' \
                 '%(C2)s,%(Volume)s,%(OI)s)'
    cursor.executemany(data_query, df.to_dict(orient="records"))
    logger.info('Inserted all record. Commit the change...')
    conn.commit()
    conn.close()
    logger.info(f"Inserted Future OI. Date:{datetime.today().strftime('%Y-%m-%d')}")