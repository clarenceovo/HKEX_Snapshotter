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

warnings.filterwarnings('ignore')
def get_tag(tag_tr)->list:
    tag = []
    for item in tag_tr.findAll("a",href=True):
        tag.append(item.contents[0])
    return tag

def content_to_df(content_str,ticker,report_date)->pd.DataFrame:
    df_list = []
    extract = [s for s in content_str.text.splitlines() if s]
    data_set = [item.replace(',', '') for item in extract if len(item)==108]
    header =['contract_month','strike','type','open','high','low','close','price_change','iv','volume','open_interest','oi_change']
    record = data_set[2:-1]
    record = [item.split(' ') for item in record]
    for item in record:
        df_list.append([i for i in item if i])

    ret_df = pd.DataFrame(df_list)
    ret_df.columns = header
    try:

        ret_df['open_interest'] = pd.to_numeric(ret_df['open_interest'])
        ret_df['volume'] = pd.to_numeric(ret_df['volume'])
        ret_df = ret_df.query('open_interest >0 & volume >0 & contract_month != "TOTAL" ')
        ret_df['contract_month'] = pd.to_datetime(ret_df['contract_month'], format="%b%y")
        ret_df['stock_id'] = int(ticker)
        ret_df['report_date'] = report_date
    except Exception as e:
        print(f'ERROR:{e}')
    return ret_df

def get_quote_list(row_list:list)->dict:
    ret = {}
    for item in row_list:
        row = item.split(' ')
        if row[-1] == '':
            break
        else:

            row = [item for item in row if item != '']
            tag = row[0]
            quote = row[-8].replace('(','').replace(')','')
            ret[tag]=int(quote)

    return ret

if __name__ == '__main__':
    db_cred = json.load(open('config/db.json'))
    conn = mysql.connector.connect(**db_cred)
    cursor = conn.cursor(buffered=True)

    start_date = datetime(2024,4,1)
    if len(sys.argv)>1:
        try:
            str_date = sys.argv[1]
            start_date = datetime.strptime(str_date,"%Y-%m-%d")
        except Exception as e:
            print(e)
            pass

    today = datetime.now().timestamp()

    while start_date.timestamp() < today:
        start_date = start_date + timedelta(days=1)
        try:
            date_str = start_date.strftime('%y%m%d')
            root_path = "https://www.hkex.com.hk/"
            content_url = f"eng/stat/dmstat/dayrpt/dqe{date_str}.htm"
            df_dict = {}
            quote_dict={}
            res = requests.get(root_path+content_url)

            res_soup = BSHTML(res.text,features="lxml")
            report_date = None
            if len(res_soup.find_all('a'))<10:
                logger.info(f'{start_date} is not available')
                continue
            for item in res_soup.find_all('a'):
                if item.get("name") is not None:
                    if item.get("name") != 'SUMMARY':
                        df = content_to_df(item,quote_dict[item.get("name")],report_date)
                        df_dict[item.get("name")]=df
                    else:
                        summary = item.text.replace('\r','')
                        row_summary = summary.split('\n')
                        date_str = ' '.join(row_summary[7].replace('\r','').split(' ')[-3:])
                        quote_dict = get_quote_list(row_summary[12:])
                        report_date = datetime.strptime(date_str, "%d %b %Y")
                        #print(report_date)

            data_query = 'INSERT IGNORE INTO stock_option_oi (stock_id,record_date,contract_month,strike,type,' \
                         'open,high,low,close,iv,open_interest,oi_change) ' \
                         'values (%(stock_id)s,%(report_date)s,%(contract_month)s,' \
                         '%(strike)s,%(type)s,%(open)s,%(high)s,%(low)s,%(close)s,' \
                         '%(iv)s,%(open_interest)s,%(oi_change)s)'
            for item in df_dict.keys():
                cursor.executemany(data_query, df_dict[item].to_dict(orient="records"))
                #logger.info(f'Inserted all record for {item} @ {start_date.strftime("%Y-%m-%d")} . Commit the change...')
                conn.commit()
            logger.info(f"SUCCESS!Data insertion for {start_date.strftime('%Y-%m-%d')} is done!")
        except Exception as e:
            logger.error(e)
            logger.info(f"Date:{start_date.strftime('%Y-%m-%d')} failed to get data ")
            continue
    conn.close()


