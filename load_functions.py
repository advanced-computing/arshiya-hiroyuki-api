import requests
from io import BytesIO
import pandas as pd
import time

def load_data(url):
    response = requests.get(url)
    cpi_data = BytesIO(response.content)
    df = pd.read_excel(cpi_data)
    return df

def get_latest_data(df, pull_date):
    year, month, day = pull_date.split('-')
    year = year[2:]
    month = str(int(month))
    col_name  = f'PCPI{year}M{month}'
    df_filtered = df[['DATE', col_name]]
    df_filtered = df_filtered.rename(columns={col_name: 'CPI'})
    return df_filtered

def update_append(pull_date, url, con):
    latest_data = get_latest_data(load_data(url), pull_date)
    latest_data = latest_data.dropna(subset=['CPI'])
    existing_dates = con.execute('SELECT DATE FROM cpi_table').fetchdf()['DATE'].tolist()
    latest_data = latest_data[~latest_data['DATE'].isin(existing_dates)]
    if not latest_data.empty:
        latest_data.to_sql('cpi_table', con, if_exists='append', index=False)

def update_truncate(pull_date, url, con):
    latest_data = get_latest_data(load_data(url), pull_date)    
    latest_data = latest_data.dropna(subset=['CPI'])
    con.execute('DELETE FROM cpi_table')
    if not latest_data.empty:
        latest_data.to_sql('cpi_table', con, if_exists='append', index=False)

def update_incremental(pull_date, url, con):
    latest_data = get_latest_data(load_data(url), pull_date)
    latest_data = latest_data.dropna(subset=['CPI'])
    existing_data = con.execute('SELECT * FROM cpi_table').fetchdf()

    if not existing_data.empty:
        merged_data = latest_data.merge(existing_data, on='DATE', suffixes=('_new', '_old'), how='left')

        updated_data = merged_data[
            (merged_data['CPI_old'].isna()) |
            (merged_data['CPI_new'] != merged_data['CPI_old'])
        ][['DATE', 'CPI_new']].rename(columns={'CPI_new': 'CPI'})

        if not updated_data.empty:
            date_list = updated_data['DATE'].tolist()
            placeholders = ', '.join(['?'] * len(date_list))
            delete_query = f'DELETE FROM cpi_table WHERE DATE IN ({placeholders})'
            con.execute(delete_query, date_list)
            updated_data.to_sql('cpi_table', con, if_exists='append', index=False)
    else:
        latest_data.to_sql('cpi_table', con, if_exists='append', index=False)

def measure_time(update_method, date_range, url, con):
    start_time = time.time()
    for pull_date in date_range:
        update_method(pull_date.strftime('%Y-%m-%d'), url, con)
    end_time = time.time()
    return end_time - start_time