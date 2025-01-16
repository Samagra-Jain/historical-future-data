import requests
import ssl
import pandas as pd
from datetime import datetime
import warnings

warnings.filterwarnings('ignore')


def segregator(instrument_name, instrument):
    df = pd.read_csv('instrument_latest.csv')
    df = df[(df['symbol'] == instrument_name) & (df['instrument_type'] == instrument) & (df['exchange1'] == 'NSEFO\n')]
    l = list(df['expiry_date'])
    unique_date_objects = sorted({datetime.strptime(date, '%d%b%Y') for date in l})
    top_2_dates = unique_date_objects[:2]
    top_2_formatted = [date.strftime('%d%b%Y').upper() for date in top_2_dates]
    df = df[df['expiry_date'].isin(top_2_formatted)].reset_index()
    expiry = {}
    for i in range(len(df)):
        expiry[df['security_id'][i]] = df['expiry_date'][i]
    return df, expiry, instrument_name


def historical_data(instrument_name, sec_id, expiry, exchange, from_date, to_date):
    """
    from_date (str): Start date in 'DD-MM-YYYY' format.
    to_date (str): End date in 'DD-MM-YYYY' format.
    """
    ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE
    from_timestamp = int(datetime.strptime(from_date, '%d-%m-%Y').timestamp())
    to_timestamp = int(datetime.strptime(to_date, '%d-%m-%Y').timestamp())
    url = (f"https://<MULTITRADE CREDS>/ticks?EXC={exchange}&SECID={sec_id}"
           f"&FROM={from_timestamp}&TO={to_timestamp}&RESOLUTION=5")
    response = requests.get(url, verify=False)
    data_json = response.json()
    data = data_json['Data']
    dataframe = {'Time' : [], 'Open' : [], 'High' : [], 'Low' : [], 'Close' : [], 'Volume' : [], 'TTQ' : []}
    for key, value in data.items():
        readable_time = datetime.fromtimestamp(int(value[0])).strftime('%Y-%m-%d %H:%M:%S')
        dataframe['Time'].append(readable_time)
        dataframe['Close'].append(value[1])
        dataframe['High'].append(value[2])
        dataframe['Low'].append(value[3])
        dataframe['Open'].append(value[4])
        dataframe['Volume'].append(value[5])
        dataframe['TTQ'].append(value[6])
    df = pd.DataFrame(dataframe)
    csv_name = f'{instrument_name}{expiry}FUT'
    df.to_csv(csv_name+'.csv')
    return df


df, expiry, instrument_name = segregator(<YOUR STOCK NAME>, 'FUTSTK')
for sec_id, expiry_date in expiry.items():
    df = historical_data(instrument_name, sec_id, expiry_date, 'NSEFO', '01-01-2025', '16-01-2025')
