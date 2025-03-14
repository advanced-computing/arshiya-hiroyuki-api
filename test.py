import pytest
import pandas as pd
import requests
from io import StringIO
from flask import Flask
from app import process_date_request, process_reason_request
from app import process_boro_request, empty_check, format_output
import duckdb

app = Flask(__name__)

# Step1: Test Functions
# Create Test Data
def load_test_data():
    data = '''Busbreakdown_ID,Occurred_On,Reason,Boro
    1,02/27/2025 10:00:00 AM,Mechanical,Manhattan
    2,02/27/2025 11:00:00 AM,Accident,Bronx
    3,02/28/2025 09:00:00 AM,Mechanical,Brooklyn
    4,03/01/2025 08:00:00 AM,Weather,Queens'''
    
    df = pd.read_csv(StringIO(data))
    df['Occurred_On'] = pd.to_datetime(
        df['Occurred_On'], 
        format='%m/%d/%Y %I:%M:%S %p').dt.date
    return df

# Test
@pytest.fixture
def df():
    return load_test_data()

def test_process_date_request(df):
    assert process_date_request(df, '02/27/2025', 'Occurred_On') == 2
    assert process_date_request(df, '03/01/2025', 'Occurred_On') == 1
    assert process_date_request(df, '03/02/2025', 'Occurred_On') == 0

def test_process_reason_request(df):
    assert process_reason_request(df, 'Mechanical', 'Reason') == 2
    assert process_reason_request(df, 'Accident', 'Reason') == 1
    assert process_reason_request(df, 'Unknown', 'Reason') == 0

def test_process_boro_request(df):
    assert process_boro_request(df, 'Manhattan', 'Boro') == 1
    assert process_boro_request(df, 'Bronx', 'Boro') == 1
    assert process_boro_request(df, 'Unknown', 'Boro') == 0

def test_empty_check():
    with app.app_context():
        df_empty = pd.DataFrame(
            columns=['Busbreakdown_ID', 'Occurred_On', 'Reason', 'Boro']
            )
        assert empty_check(df_empty) is not None

        df_non_empty = load_test_data()
        assert empty_check(df_non_empty) is None

def test_format_output(df):
    with app.app_context():
        output_csv = format_output(df, 'csv')
        assert output_csv.startswith('Busbreakdown_ID,Occurred_On,Reason,Boro')

        output_json = format_output(df, 'json')
        assert '"Busbreakdown_ID":' in output_json

# Step2: Test APIs 
# Run app.py in the background.
# Set Test Environment and functions.
api_url = "http://127.0.0.1:5000"

def clean_data(data):
    data = data.map(lambda x: None if str(x).lower() in ['nan', 'none'] else x)
    data = data.reset_index(drop=True)
    return data

# Set Actual Downloaded Data
@pytest.fixture
def dataframe():
    con = duckdb.connect('bus.db')
    df = con.execute('SELECT * FROM bus').fetchdf()
    con.close()
    df['Occurred_On'] = pd.to_datetime(df['Occurred_On'], 
                                        format='%m/%d/%Y %I:%M:%S %p').dt.date
    df = clean_data(df)
    return df

# Test API of Count records by date:
def test_get_date_data(dataframe):
    test_date = dataframe.iloc[0]['Occurred_On']
    # test_date = datetime.date(2024, 9, 3) if you want to manually enter a date
    expected_count = dataframe[dataframe['Occurred_On'] == test_date].shape[0]
    
    test_url = f'{api_url}/date?date={test_date}'
    response = requests.get(test_url).json()

    assert expected_count == response['count']

# Test API of Count records by reason:
def test_get_reason_data(dataframe):
    test_reason = dataframe.iloc[0]['Reason']
    expected_count = dataframe[dataframe['Reason'] == test_reason].shape[0]

    test_url = f'{api_url}/reason?reason={test_reason}'
    response = requests.get(test_url).json()

    assert expected_count == response['count']

# Test API of Count records by borough:
def test_get_boro_data():
    test_boro = "NotaRealBoro"
    expected_count = 2

    test_url = f'{api_url}/boro?boro={test_boro}'
    response = requests.get(test_url).json()

    assert expected_count != response['count']
        
# Step3: Test Data Quality
def test_id_completeness(dataframe):
    assert not dataframe['Busbreakdown_ID'].isnull().any()

def test_date_type(dataframe):
    assert all(isinstance(x, pd.Timestamp) for x in pd.to_datetime(
        dataframe['Occurred_On'], errors='coerce'))

def test_date_range(dataframe):
    start_date = pd.Timestamp('2024-09-03')
    end_date = pd.Timestamp('2025-02-14')
    dataframe['Occurred_On'] = pd.to_datetime(dataframe['Occurred_On'])
    assert all((dataframe['Occurred_On'] >= start_date) 
               & (dataframe['Occurred_On'] <= end_date))
    
def test_boro_category(dataframe):
    boro_categories = ['Brooklyn', 'Bronx', 'Queens', 'Manhattan',
                       'Nassau County', 'Staten Island', 'Westchester',
                       'All Boroughs', 'New Jersey', 'Rockland County',
                       'Connecticut'
                       ]
    sum_boro = 44077 
    count = 0
    for boro in boro_categories:
        num = dataframe[dataframe['Boro'] == boro].shape[0]
        count += num
    assert count == sum_boro

def test_reason_category(dataframe):
    reason_categories = ['Heavy Traffic', 'Mechanical Problem', 'Problem Run',
                         'Delayed by School', 'Won`t Start', 'Accident',
                         'Flat Tire', 'Weather Conditions',
                         'Late return from Field Trip','Other',
                         ]
    sum_reason = 44081 
    count = 0
    for reason in reason_categories:
        num = dataframe[dataframe['Reason'] == reason].shape[0]
        count += num
    assert count == sum_reason

