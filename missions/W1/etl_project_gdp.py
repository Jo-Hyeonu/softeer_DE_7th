import requests
from bs4 import BeautifulSoup
import pandas as pd
import sqlite3
from datetime import datetime
from tabulate import tabulate

#timestamp
def current_time()->datetime:
    return datetime.now().strftime("%Y-%B-%d-%H-%M-%S")

#logstamp
def log_press(message):
    timestamp = current_time()
    with open('etl_gdp_log.txt', 'a', encoding='utf-8') as f:
        f.write(f"{timestamp}, {message}\n")
    
#crawl gdp data
def extract():
    log_press("Start Extract GDP DATA")
    url = "https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29"
    

    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebkit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36' 
    }
    try:
        response = requests.get(url,headers=headers)
        status = response.status_code
        
        if status == 200:
            #raw html 저장하기
            with open(f'{current_time()}_etl_gdp_rawdata.html', 'w',encoding='utf-8') as f:
                f.write(response.text)

            soup = BeautifulSoup(response.text, 'html.parser')
            table = soup.select_one(".wikitable.sortable.sticky-header-multi")

            data_list = []
            rows = table.find_all('tr')
            for row in rows:
                cols = row.find_all('td')
                if len(cols) > 2:
                    country = cols[0].get_text(strip=True)
                    gdp = cols[1].get_text(strip=True)
                    if country == 'World':
                        continue
                    record = {
                        'Country' : country,
                        'GDP_USD_billion' : gdp
                    }
                    data_list.append(record)
            df = pd.DataFrame(data_list)
            log_press("End Extract GDP DATA")
            return df
        else:
            log_press(f"ERROR : while extract() {status} error!")
    except Exception as e:
        log_press("ERROR : {e}")
    return None

#transform data (mil->bil)
def transform(df):
    log_press("Start Transform GDP DATA")

    df['Country'] = df['Country'].str.replace(r'\[.*?\]', '', regex= True).str.strip()

    df['GDP_USD_billion'] = df['GDP_USD_billion'].astype(str)
    df['GDP_USD_billion'] = df['GDP_USD_billion'].str.split('(').str[0]
    df['GDP_USD_billion'] = df['GDP_USD_billion'].str.replace(',','')
    df['GDP_USD_billion'] = pd.to_numeric(df['GDP_USD_billion'], errors='coerce')
    df['GDP_USD_billion'] = round((df['GDP_USD_billion'] / 1000),2)
    df = df.dropna(subset=['GDP_USD_billion'])

    df['Timestamp'] = current_time()
    
    country_map = {
        # Africa
        'South Africa': 'Africa', 'Egypt': 'Africa', 'Algeria': 'Africa', 'Nigeria': 'Africa', 
        'Morocco': 'Africa', 'Kenya': 'Africa', 'Angola': 'Africa', 'Ghana': 'Africa', 
        'Ethiopia': 'Africa', 'Ivory Coast': 'Africa', 'Tanzania': 'Africa', 'DR Congo': 'Africa', 
        'Uganda': 'Africa', 'Cameroon': 'Africa', 'Tunisia': 'Africa', 'Zimbabwe': 'Africa', 
        'Libya': 'Africa', 'Senegal': 'Africa', 'Sudan': 'Africa', 'Zambia': 'Africa', 
        'Guinea': 'Africa', 'Burkina Faso': 'Africa', 'Mali': 'Africa', 'Mozambique': 'Africa', 
        'Benin': 'Africa', 'Niger': 'Africa', 'Chad': 'Africa', 'Gabon': 'Africa', 
        'Madagascar': 'Africa', 'Botswana': 'Africa', 'Mauritius': 'Africa', 'Congo': 'Africa', 
        'Malawi': 'Africa', 'Rwanda': 'Africa', 'Namibia': 'Africa', 'Equatorial Guinea': 'Africa', 
        'Somalia': 'Africa', 'Mauritania': 'Africa', 'Togo': 'Africa', 'Sierra Leone': 'Africa', 
        'Burundi': 'Africa', 'Eswatini': 'Africa', 'Liberia': 'Africa', 'South Sudan': 'Africa', 
        'Djibouti': 'Africa', 'Central African Republic': 'Africa', 'Cape Verde': 'Africa', 
        'Gambia': 'Africa', 'Zanzibar': 'Africa', 'Guinea-Bissau': 'Africa', 'Lesotho': 'Africa', 
        'Eritrea': 'Africa', 'Seychelles': 'Africa', 'Comoros': 'Africa', 'São Tomé and Príncipe': 'Africa',

        # Europe
        'Germany': 'Europe', 'United Kingdom': 'Europe', 'France': 'Europe', 'Italy': 'Europe', 
        'Russia': 'Europe', 'Spain': 'Europe', 'Netherlands': 'Europe', 'Poland': 'Europe', 
        'Switzerland': 'Europe', 'Belgium': 'Europe', 'Ireland': 'Europe', 'Sweden': 'Europe', 
        'Austria': 'Europe', 'Norway': 'Europe', 'Denmark': 'Europe', 'Romania': 'Europe', 
        'Czech Republic': 'Europe', 'Portugal': 'Europe', 'Finland': 'Europe', 'Greece': 'Europe', 
        'Hungary': 'Europe', 'Ukraine': 'Europe', 'Slovakia': 'Europe', 'Bulgaria': 'Europe', 
        'Croatia': 'Europe', 'Luxembourg': 'Europe', 'Serbia': 'Europe', 'Lithuania': 'Europe', 
        'Belarus': 'Europe', 'Slovenia': 'Europe', 'Latvia': 'Europe', 'Estonia': 'Europe', 
        'Cyprus': 'Europe', 'Iceland': 'Europe', 'Bosnia and Herzegovina': 'Europe', 'Albania': 'Europe', 
        'Malta': 'Europe', 'Moldova': 'Europe', 'North Macedonia': 'Europe', 'Kosovo': 'Europe', 
        'Channel Islands': 'Europe', 'Monaco': 'Europe', 'Liechtenstein': 'Europe', 
        'Montenegro': 'Europe', 'Isle of Man': 'Europe', 'Andorra': 'Europe', 'Faroe Islands': 'Europe', 
        'San Marino': 'Europe',

        # Asia
        'China': 'Asia', 'Japan': 'Asia', 'India': 'Asia', 'South Korea': 'Asia', 'Turkey': 'Asia', 
        'Indonesia': 'Asia', 'Saudi Arabia': 'Asia', 'Taiwan': 'Asia', 'Israel': 'Asia', 
        'Singapore': 'Asia', 'United Arab Emirates': 'Asia', 'Thailand': 'Asia', 'Philippines': 'Asia', 
        'Vietnam': 'Asia', 'Bangladesh': 'Asia', 'Malaysia': 'Asia', 'Hong Kong': 'Asia', 
        'Pakistan': 'Asia', 'Iran': 'Asia', 'Kazakhstan': 'Asia', 'Iraq': 'Asia', 'Qatar': 'Asia', 
        'Kuwait': 'Asia', 'Uzbekistan': 'Asia', 'Oman': 'Asia', 'Sri Lanka': 'Asia', 
        'Azerbaijan': 'Asia', 'Turkmenistan': 'Asia', 'Myanmar': 'Asia', 'Jordan': 'Asia', 
        'Macau': 'Asia', 'Cambodia': 'Asia', 'Bahrain': 'Asia', 'Nepal': 'Asia', 'Georgia': 'Asia', 
        'Lebanon': 'Asia', 'Armenia': 'Asia', 'Mongolia': 'Asia', 'Kyrgyzstan': 'Asia', 
        'Syria': 'Asia', 'Afghanistan': 'Asia', 'Tajikistan': 'Asia', 'Laos': 'Asia', 
        'North Korea': 'Asia', 'Brunei': 'Asia', 'Palestine': 'Asia', 'Yemen': 'Asia', 
        'Maldives': 'Asia', 'Bhutan': 'Asia', 'Timor-Leste': 'Asia',

        # North-America
        'United States': 'North-America', 'Canada': 'North-America', 'Mexico': 'North-America', 
        'Cuba': 'North-America', 'Dominican Republic': 'North-America', 'Puerto Rico': 'North-America', 
        'Guatemala': 'North-America', 'Costa Rica': 'North-America', 'Panama': 'North-America', 
        'Honduras': 'North-America', 'El Salvador': 'North-America', 'Haiti': 'North-America', 
        'Trinidad and Tobago': 'North-America', 'Jamaica': 'North-America', 'Nicaragua': 'North-America', 
        'Bahamas': 'North-America', 'Bermuda': 'North-America', 'Barbados': 'North-America', 
        'Cayman Islands': 'North-America', 'U.S. Virgin Islands': 'North-America', 'Aruba': 'North-America', 
        'Greenland': 'North-America', 'Belize': 'North-America', 'Curaçao': 'North-America', 
        'Saint Lucia': 'North-America', 'Antigua and Barbuda': 'North-America', 
        'Turks and Caicos Islands': 'North-America', 'Sint Maarten': 'North-America', 
        'British Virgin Islands': 'North-America', 'Grenada': 'North-America', 
        'Saint Vincent and the Grenadines': 'North-America', 'Saint Kitts and Nevis': 'North-America', 
        'Dominica': 'North-America', 'Saint Martin': 'North-America', 'Anguilla': 'North-America', 
        'Montserrat': 'North-America',

        # South-America
        'Brazil': 'South-America', 'Argentina': 'South-America', 'Colombia': 'South-America', 
        'Chile': 'South-America', 'Peru': 'South-America', 'Ecuador': 'South-America', 
        'Uruguay': 'South-America', 'Venezuela': 'South-America', 'Bolivia': 'South-America', 
        'Paraguay': 'South-America', 'Guyana': 'South-America', 'Suriname': 'South-America',

        # Oceania
        'Australia': 'Oceania', 'New Zealand': 'Oceania', 'Papua New Guinea': 'Oceania', 
        'New Caledonia': 'Oceania', 'Guam': 'Oceania', 'French Polynesia': 'Oceania', 
        'Fiji': 'Oceania', 'Solomon Islands': 'Oceania', 'Samoa': 'Oceania', 'Vanuatu': 'Oceania', 
        'Northern Mariana Islands': 'Oceania', 'American Samoa': 'Oceania', 'Tonga': 'Oceania', 
        'Micronesia': 'Oceania', 'Cook Islands': 'Oceania', 'Palau': 'Oceania', 
        'Kiribati': 'Oceania', 'Marshall Islands': 'Oceania', 'Nauru': 'Oceania', 'Tuvalu': 'Oceania'
    }
    df['Region'] = df['Country'].map(country_map).fillna("Others")

    log_press("End Transform GDP DATA")
    #print(df)
    return df

def load_json(df):
    log_press("Start Load GDP DATA to json")
    df.to_json('Countries_by_GDP.json', orient='records', indent=4)
    log_press("End Load GDP DATA to json")

def load_db(df):
    log_press("Start Load GDP DATA to DB")
    with sqlite3.connect('World_Economies.db') as conn:
        df.to_sql('Countries_by_GDP', conn, if_exists='replace', index=False)
    log_press("End Load GDP DATA to DB")

def run_query():
    with sqlite3.connect('World_Economies.db') as conn:
        data_over_100B = pd.read_sql_query('''
                    SELECT Country, GDP_USD_billion 
                    FROM Countries_by_GDP
                    WHERE GDP_USD_billion >= 100
                    ORDER BY GDP_USD_billion DESC
                    ''',conn)
        print("--------- Countries with GDP > 100 ----------")
        print(tabulate(
            data_over_100B,
            headers='keys',
            tablefmt='pretty',
            showindex=False
        ))
        data_top5_avg = pd.read_sql_query('''
                    SELECT Region, ROUND(AVG(GDP_USD_billion),2) as average
                    FROM (
                        select Region, Country, GDP_USD_billion,
                        ROW_NUMBER() OVER (
                            PARTITION BY Region
                            ORDER BY GDP_USD_billion DESC
                        ) AS rn
                        FROM Countries_by_GDP
                    )
                    WHERE rn <= 5
                    GROUP BY Region
                    ORDER BY average DESC
                    ''',conn)
        print("--------- Avg GDP of Top 5 Countries by Region ----------")
        print(tabulate(data_top5_avg, headers='keys', tablefmt='pretty', showindex=False))


def run():
    #ETL
    df = extract()
    if df is not None:
        df = transform(df)
        load_db(df)
        load_json(df)
        run_query()
    
if __name__ == "__main__":
    run()