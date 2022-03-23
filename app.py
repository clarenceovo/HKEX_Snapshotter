import requests
import json
import mysql.connector
from datetime import datetime
from bs4 import BeautifulSoup
import logging
import sys
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


if __name__ == '__main__':
    #For Dev , read from local
    body = []
    result =[]
    name_list =[]
    mode = sys.argv[1]
    timeslot = sys.argv[2] #AM or PM
    print(sys.argv)

    if mode == "DEV":
        file  = open('ashtmain.htm')
        soup = BeautifulSoup(file,'html.parser')

    elif mode == "PROD":

        #TODO: Get the htm from HKEX site directly
        if timeslot == 'AM':
            web_url = 'https://www.hkex.com.hk/eng/stat/smstat/ssturnover/ncms/mshtmain.htm'
        else:
            web_url = 'https://www.hkex.com.hk/eng/stat/smstat/ssturnover/ncms/ashtmain.htm'
        ret = requests.get(web_url).text
        soup=BeautifulSoup(ret,'html.parser')
    #print(ret)
    logger.info(f'MODE:{mode}. Session:{timeslot}')
    #Get the body content
    data_body = soup.find('body').findChildren(recursive=False)[0].get_text()
    #Split by line breaker
    data = data_body.split('\n')
    if len(data) >20: #To filter out not available result

        record_date = data[2]
        record_body = data[7:]
        record_date = record_date.split(' ')
        date_str = ' '.join(record_date[-4:-1])
        dt = datetime.strptime(date_str,'%d %b %Y').strftime("%Y-%m-%d")
        logger.info(f'Trading date:{dt}. Session:{timeslot}')
        for item in record_body:
            if len(item) == 0:
                break
            if item[0]== 'T':
                continue
            body.append(item)
        for record in body:
            record = record.replace('\r','')
            tmp = record.split(' ')
            tmp = [x for x in tmp if x]
            if len(tmp)>0:
                if tmp[0] == '%':
                    tmp.pop(0)
                try:
                    turnover = tmp.pop(-1)
                    turnover = float(''.join(turnover.split(',')))
                    num_share = tmp.pop(-1)
                    num_share = float(''.join(num_share.split(',')))
                    ticker = tmp.pop(0)
                    ticker = ''.join(ticker.split('%'))
                    name = ' '.join(tmp)
                    name_list.append((ticker,name))
                    result.append((ticker,dt,timeslot,num_share,turnover))
                except Exception as e:
                    logger.error(e)
        db_cred = json.load(open('config/db.json'))

        #init MYSQL connection
        conn = mysql.connector.connect(**db_cred)
        cursor = conn.cursor(buffered=True)

        try:
            logger.info("Inserting Name")
            name_insert_qry = 'INSERT IGNORE INTO ticker_name (ticker,name) values (%s,%s)'
            cursor.executemany(name_insert_qry,name_list)
            logger.info("Inserted Stock list. Execute insert data query")

            logger.info("Inserting Shortselling")
            data_insert_qry = "INSERT IGNORE into short_selling (ticker,date,session,shares,turnover)values (%s,%s,%s,%s,%s)"
            cursor.executemany(data_insert_qry,result)

            logger.info('Inserted all record. Commit the change...')
            conn.commit()
            logger.info("Insertion Complete. Closing the program")
            conn.close()
        except Exception as e:
            logger.fatal('Database insertion interrupted. Please see the error message below')
            logger.fatal(e)
        #Insert the record into DB

    else:
        print(data)
        #logger.error('The result is not available in HKEX website.')
