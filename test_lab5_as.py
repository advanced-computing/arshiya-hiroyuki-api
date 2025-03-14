import pandas as pd
import requests
import datetime
from app import get_date_data, get_reason_data, get_boro_data

api_url = "http://127.0.0.1:5000"

df = pd.read_csv("bus_2024_2025.csv")
df['Occurred_On'] = pd.to_datetime(df['Occurred_On'], format='%m/%d/%Y %I:%M:%S %p', errors='coerce')

def test_get_date_data():
    test_date = df.iloc[0]['Occurred_On'].date()
    #test_date = datetime.date(2024,9,3) if you want to manually enter a date
    expected_count = df[df['Occurred_On'].dt.date == test_date].shape[0]
    
    test_url = f'{api_url}/date?date={test_date}'
    response = requests.get(test_url).json()

    assert expected_count == response["count"]

def test_get_reason_data():
    test_reason = df.iloc[0]['Reason']
    expected_count = df[df['Reason'] == test_reason].shape[0]

    test_url = f'{api_url}/reason?reason={test_reason}'
    response = requests.get(test_url).json()

    assert expected_count == response["count"]
    
def test_get_boro_data():
    test_boro = "NotaRealBoro"
    expected_count = 2

    test_url = f'{api_url}/boro?boro={test_boro}'
    response = requests.get(test_url).json()

    assert expected_count != response["count"]


