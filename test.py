import pytest
import pandas as pd
import requests
import io
from io import StringIO
from flask import Flask
from app import process_date_request, process_reason_request
from app import process_boro_request, empty_check, format_output

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
    df = pd.read_csv('bus_2024_2025.csv')
    df['Occurred_On'] = pd.to_datetime(
        df['Occurred_On'],
        format='%m/%d/%Y %I:%M:%S %p',
        errors='coerce'
        ).dt.date
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

# Test API of Fetch all delays for a specific date (JSON format):
def get_expected_data_records(dataframe, dates):
    expected_data = []
    for test_date in dates:
        date = pd.to_datetime(test_date).date()
        test = dataframe[dataframe['Occurred_On'] == date]
        test = clean_data(test)
        expected_data.append(test)
    return expected_data

def get_output_data_records_json(dates):
    output_data = []
    for test_date in dates:
        date = pd.to_datetime(test_date).date()
        url = f'{api_url}/date_records?date={date}&format=json'
        response = requests.get(url)
        response_data = response.json()
        
        if isinstance(response_data, list):
            api_data = pd.DataFrame(response_data)
        else:
            api_data = pd.DataFrame([response_data])

        if 'Occurred_On' in api_data.columns:
            api_data['Occurred_On'] = pd.to_datetime(
                api_data['Occurred_On'], unit='ms').dt.date
        else:
            api_data = pd.DataFrame(columns=['Occurred_On'])

        api_data = clean_data(api_data)
        output_data.append(api_data)
    return output_data

def test_date_records_json(dataframe):
    true_dates = ['2024-09-25', '2024-09-26', '2024-09-27']
    false_dates = ['2020-09-25', '2027-09-27']
    
    true_expected_data = get_expected_data_records(dataframe, true_dates)
    true_output_data = get_output_data_records_json(true_dates)
    for expected, output in zip(true_expected_data, true_output_data):
        assert expected.equals(output)
    
    false_expected_data = get_expected_data_records(dataframe, false_dates)
    false_output_data = get_output_data_records_json(false_dates)
    for expected, output in zip(false_expected_data, false_output_data):
        assert not expected.equals(output)
    
# Test API of Fetch all delays for a specific date (CSV format):
def get_output_data_records_csv(dates):
    output_data = []
    for test_date in dates:
        date = pd.to_datetime(test_date).date()
        url = f'{api_url}/date_records?date={date}&format=csv'
        response = requests.get(url)
        response_data = response.text
        api_data = pd.read_csv(io.StringIO(response_data))
        
        if 'Occurred_On' in api_data.columns:
            api_data['Occurred_On'] = pd.to_datetime(
                api_data['Occurred_On'], format='%Y-%m-%d').dt.date
        else:
            api_data = pd.DataFrame(columns=['Occurred_On'])

        api_data = clean_data(api_data)
        output_data.append(api_data)
    return output_data

def test_date_records_csv(dataframe):
    true_dates = ['2024-09-25', '2024-09-26', '2024-09-27']
    false_dates = ['2020-09-25', '2027-09-27']
    
    true_expected_data = get_expected_data_records(dataframe, true_dates)
    true_output_data = get_output_data_records_csv(true_dates)
    for expected, output in zip(true_expected_data, true_output_data):
        assert expected.equals(output)
    
    false_expected_data = get_expected_data_records(dataframe, false_dates)
    false_output_data = get_output_data_records_csv(false_dates)
    for expected, output in zip(false_expected_data, false_output_data):
        assert not expected.equals(output)
        
# Test API of Fetch records with pagination:
def test_pagination(dataframe):
    format = 'json'
    column = 'Reason'
    value = 'Heavy Traffic'
    limit = 5
    offset = 0
    
    test = dataframe[dataframe[column] == value].iloc[offset:offset + limit]
    
    url = (
    f'{api_url}/records?format={format}'
    f'&column={column}&value={value}'
    f'&limit={limit}&offset={offset}'
    )
    
    response = requests.get(url)
    
    if format == 'json':
        api_data = pd.DataFrame(response.json())
        api_data['Occurred_On'] = pd.to_datetime(
            api_data['Occurred_On'], unit='ms').dt.date
    elif format == 'csv':
        api_data = pd.read_csv(io.StringIO(response.text))
        api_data['Occurred_On'] = pd.to_datetime(
            api_data['Occurred_On'], format='%Y-%m-%d').dt.date

    test = clean_data(test)
    api_data = clean_data(api_data)
    
    assert test.equals(api_data)
    
# Test API of Fetch a specific record by ID:
def test_id(dataframe):
    true_ids = [1933906, 1933907, 1933908]
    false_ids = [1, 9999999]
    for test_id in true_ids:
        test = dataframe[dataframe['Busbreakdown_ID'] == test_id]

        url = f'{api_url}/record/{test_id}?format=json'
        response = requests.get(url)
        api_data = pd.DataFrame(response.json())
        api_data['Occurred_On'] = pd.to_datetime(
            api_data['Occurred_On'], unit='ms').dt.date

        test = clean_data(test)
        api_data = clean_data(api_data)

        assert test.equals(api_data)

    for test_id in false_ids:
        test = dataframe[dataframe['Busbreakdown_ID'] == test_id]

        url = f'{api_url}/record/{test_id}?format=json'
        response = requests.get(url)
        response_data = response.json()
        if response_data.get('message') == 'No Records':
            api_data = pd.DataFrame(columns=test.columns)
        else:
            api_data = pd.DataFrame([response_data])
            if 'Occurred_On' in api_data.columns:
                api_data['Occurred_On'] = pd.to_datetime(
                    api_data['Occurred_On'], unit='ms').dt.date

        test = clean_data(test)
        api_data = clean_data(api_data)

        assert test.empty
        assert api_data.empty
        
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

