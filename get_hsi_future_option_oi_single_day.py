import pandas as pd
import requests
import json
from datetime import datetime ,timedelta
from bs4 import BeautifulSoup as BSHTML
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
pd.set_option('display.expand_frame_repr', False)
import sys
import mysql.connector
import warnings


def process_data(content,report_date):
    header = ['contract_month', 'strike', 'type', 'open', 'high', 'low', 'close', 'price_change', 'iv',"d1volume", 'volume',
              'open_interest', 'oi_change']
    content = content.replace('\r','')
    content = content.split('\n')
    result=[]
    for row in content[3:]:
        if len(row)>10:
            row = [x for x in row.split(' ') if x and x != '|']
            if '-'  in row[0]:
                try:
                    result.append([row[0],int(row[1])
                                      ,row[2],row[8],row[9],row[10],row[11],row[12],row[13],int(row[7]),int(row[14]),row[18],row[19]])
                except:
                    pass
    ret = pd.DataFrame(result)
    ret.columns = header
    ret['report_date']=report_date

    return ret

if __name__ == '__main__':
    db_cred = json.load(open('config/db.json'))
    conn = mysql.connector.connect(**db_cred)
    cursor = conn.cursor(buffered=True)

    start_date = datetime(2022,4,20)
    if len(sys.argv)>1:
        try:
            str_date = sys.argv[1]
            start_date = datetime.strptime(str_date,"%Y-%m-%d")
        except Exception as e:
            print(e)
            pass




    start_date = datetime.utcnow()+timedelta(hours=8)
    try:
        date_str = start_date.strftime('%y%m%d')
        root_path = "https://www.hkex.com.hk/"
        content_url = f"eng/stat/dmstat/dayrpt/hsio{date_str}.htm"
        df_dict = {}
        quote_dict={}
        res = requests.get(root_path+content_url)
        res_soup = BSHTML(res.text,features="lxml")
        report_date = None
        if len(res_soup.find_all('a')) < 10:
            logger.info(f'{start_date.strftime("%Y-%m-%d")} is not available')
            sys.exit(0)
        #for item in res_soup.find_all('a'):
        #    print(item)
        data_query = 'INSERT IGNORE INTO index_option_oi (record_date,contract_month,strike,type,' \
                     'open,high,low,close,night_volume,volume,iv,open_interest,oi_change) ' \
                     'values (%(report_date)s,%(contract_month)s,' \
                     '%(strike)s,%(type)s,%(open)s,%(high)s,%(low)s,%(close)s,' \
                     '%(d1volume)s,%(volume)s,' \
                     '%(iv)s,%(open_interest)s,%(oi_change)s)'
        logger.info(f"Inserting data for {start_date.strftime('%Y-%m-%d')}.....")
        for item in res_soup.findAll('a'):
            if item.get("name") in ['month1','month2']:
                df = process_data(item.next,start_date.strftime('%Y-%m-%d'))
                df['contract_month'] = pd.to_datetime(df['contract_month'], format="%b-%y")
                cursor.executemany(data_query, df.to_dict(orient="records"))
        conn.commit()
        logger.info(f"SUCCESS!Data insertion for {start_date.strftime('%Y-%m-%d')} is done!")


    except Exception as e:
        pass
        logger.error(e)