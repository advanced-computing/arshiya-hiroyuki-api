import pandas as pd
import requests
import io

df = pd.read_csv("bus_2024_2025.csv")
df['Occurred_On'] = pd.to_datetime(df['Occurred_On'], format='%m/%d/%Y %I:%M:%S %p').dt.date

def clean_data(data):
    data = data.astype(str).replace(['NaN', 'nan', 'None'], [None, None, None])
    data = data.reset_index(drop=True)
    return data

def assert_date(api_data, test, test_date):
    try:
        assert test.equals(api_data)
        print(f'Test passed for date: {test_date}')
    except AssertionError:
        if test.empty and api_data.empty:
            print(f'No data for date: {test_date}. Test considered successful.')
        else:
            print(f'Test failed for date: {test_date}')

# GET /date_records?date=2024-09-27&format=json
def test_date_records_json(dates):
    for test_date in dates:
        date = pd.to_datetime(test_date).date()
        test = df[df['Occurred_On'] == date]
        url = f'http://127.0.0.1:5000/date_records?date={date}&format=json'
        response = requests.get(url)        
        response_data = response.json()
        
        if isinstance(response_data, list):
            api_data = pd.DataFrame(response_data)
        else:
            api_data = pd.DataFrame([response_data])

        if 'Occurred_On' in api_data.columns:
            api_data['Occurred_On'] = pd.to_datetime(api_data['Occurred_On'], unit='ms').dt.date
        else:
            api_data = pd.DataFrame(columns=['Occurred_On'])

        test = clean_data(test)
        api_data = clean_data(api_data)
        
        assert_date(api_data, test, test_date)

# GET /date_records?date=2024-09-27&format=csv
def test_date_records_csv(dates):
    for test_date in dates:
        date = pd.to_datetime(test_date).date()
        test = df[df['Occurred_On'] == date]
        url = f'http://127.0.0.1:5000/date_records?date={date}&format=csv'
        response = requests.get(url)
        api_data = pd.read_csv(io.StringIO(response.text))

        if 'Occurred_On' in api_data.columns:
            api_data['Occurred_On'] = pd.to_datetime(api_data['Occurred_On'], format='%Y-%m-%d').dt.date
        else:
            api_data = pd.DataFrame(columns=['Occurred_On'])

        test = clean_data(test)
        api_data = clean_data(api_data)
    
        assert_date(api_data, test, test_date)

# GET /records?format=json&column=Reason&value=Heavy%20Traffic&limit=5&offset=0
def test_pagination(format, column, value, limit, offset):
    test = df[df[column]==value].iloc[offset:offset+limit]

    url = f'http://127.0.0.1:5000/records?format={format}&column={column}&value={value}&limit={limit}&offset={offset}'
    response = requests.get(url)
    
    if format == 'json':
        api_data = pd.DataFrame(response.json())
        api_data['Occurred_On'] = pd.to_datetime(api_data['Occurred_On'], unit='ms').dt.date
    elif format == 'csv':
        api_data = pd.read_csv(io.StringIO(response.text))
        api_data['Occurred_On'] = pd.to_datetime(api_data['Occurred_On'], format='%Y-%m-%d').dt.date

    test = clean_data(test)
    api_data = clean_data(api_data)
    
    try:
        assert test.equals(api_data)
        print(f'Test passed for format: {format}, column: {value}, column: {limit}, offset: {offset}')
    except AssertionError:
        if test.empty and api_data.empty:
            print(f'No data for format:  {format}, column: {value}, column: {limit}, offset: {offset}. Test considered successful.')
        else:
            print(f'Test failed for format:  {format}, column: {value}, column: {limit}, offset: {offset}')
             

# GET /record/{id}?format=json
def test_id(ids):
    for test_id in ids:
        test = df[df['Busbreakdown_ID'] == test_id]

        url = f'http://127.0.0.1:5000/record/{test_id}?format=json'
        response = requests.get(url)
        api_data = pd.DataFrame(response.json())
        api_data['Occurred_On'] = pd.to_datetime(api_data['Occurred_On'], unit='ms').dt.date

        test = clean_data(test)
        api_data = clean_data(api_data)

        assert test.equals(api_data)
        print(f'Test passed for ID: {test_id}')

def run_all_tests():
    dates = ['2024-09-26', '2024-09-27', '2024-09-28']
    test_date_records_json(dates)
    test_date_records_csv(dates)

    formats = ['json', 'csv']
    columns = ['Reason']
    values = ['Heavy Traffic', 'Mechanical Problem']
    limits = [5, 10]
    offsets = [0, 5] 

    for format in formats:
        for column in columns:
            for value in values:
                for limit in limits:
                    for offset in offsets:
                        test_pagination(format, column, value, limit, offset)
    
    ids_to_test = [1933906, 1933907, 1933908]
    test_id(ids_to_test)

run_all_tests()