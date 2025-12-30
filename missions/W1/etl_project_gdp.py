import requests
from bs4 import BeautifulSoup
import pandas as pd
import sqlite3
import json
from datetime import datetime

#timestamp
def current_time()->datetime:
    return datetime.now().strftime("%Y-%m-%D-%H-%M-%S")

#logstamp
def log_press(message):
    timestamp = current_time()
    with open('etl_gdp_log.txt', 'a', encoding='utf-8') as f:
        f.write(f"{timestamp} - {message}")

def init_db():
    conn = sqlite3.connet('World Economics')
    cur = conn.cursor()
    cur.execute("""CREATE TABLE if not exists Countreis_by_GDP
                (CountryName TEXT PRIMARY KEY,
                Region TEXT,
                GDP_USD_billion FLOAT,
                Timestamp DATE,
                )
                """)

    conn.commit()
    conn.close()
    
#crawl gdp data
def extract():
    log_press("Extract GDP DATA")
    url = "https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebkit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36' 
    }
    response = requests.get(url,headers=headers)
    status = response.status_code

    if status == 200:
        soup = BeautifulSoup(response.text, 'html.parser')
        table = soup.select_one(".wikitable.sortable.sticky-header-multi")

        print(table) #debug
        with open("crawl_gdp_rawdata.txt", 'w', encoding='utf-8') as f:
            f.write(table)
    else:
        log_press(f"ERROR : while extract() {status} error!")


#transform data (mil->bil)
def transform():
    log_press("Transform GDP DATA")

def load_json():
    log_press("Load GDP DATA to json")
    pass

def load_db():
    log_press("Load GDP DATA to DB")
    pass

#rcv user query
def run_query():
    cur.execute("""
                SELECT AVG(GDP_USD_billion) 
                FROM Countreis_by_GDP
                GROUP BY Region
                HAVING 5 limit
                """)
    ret = cur.fetchall()
    print(ret)

def run():
    #init rdb
    init_db()

    #ETL
    extract()
    transform()
    load_db()
    load_json()

    #show result
    run_query()
    
if __name__ == "main":
    run()