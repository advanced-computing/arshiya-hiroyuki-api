from flask import Flask, jsonify, request
import duckdb
import pandas as pd

app = Flask(__name__)

# Functions
def create_users_table():
    con = duckdb.connect('bus.db')
    con.execute('''
    CREATE TABLE IF NOT EXISTS users (
        username TEXT,
        age INTEGER,
        country TEXT
    )
    ''')
    con.close()
    
create_users_table()
    
def clean_data(data):
    data = data.map(lambda x: None if str(x).lower() in ['nan', 'none'] else x)
    data = data.reset_index(drop=True)
    return data

def load_data():
    con = duckdb.connect('bus.db')
    df = con.execute('SELECT * FROM bus').fetchdf()
    con.close()
    df['Occurred_On'] = pd.to_datetime(df['Occurred_On'], 
                                        format='%m/%d/%Y %I:%M:%S %p').dt.date
    df = clean_data(df)
    return df

def process_date_request(df, date, column):        
    date = pd.to_datetime(date).date()
    num = df[df[column] == date].shape[0]
    return num

def process_reason_request(df, reason, column):
    num = df[df[column] == reason].shape[0]
    return num

def process_boro_request(df, boro, column):
    num = df[df[column] == boro].shape[0]
    return num

def empty_check(result):
    if result.empty:
        return jsonify(message='No Records')

def format_output(df, output_format):
    if output_format == 'csv':
        return df.to_csv(index=False)
    else:
        return df.to_json(orient='records')

# APIs
@app.route('/')
def echo():
    print('-----START-----')
    print('Method:', request.method)
    print('URL:', request.url)
    print('Headers:\n')
    print(request.headers)
    print(f'Body: {request.get_data(as_text=True)}')
    print('-----END-----')
    return 'see console'

@app.route('/users', methods=['POST'])
def add_user():
    data = request.get_json()
    username = data.get('username')
    age = data.get('age')
    country = data.get('country')

    if not username or not isinstance(age, int) or age <= 0 or not country:
        return jsonify({'error': 'Invalid input'}), 400

    with duckdb.connect('bus.db') as con:
        con.execute(
            'INSERT INTO users (username, age, country) VALUES (?, ?, ?)',
            (username, age, country)
            )

    return jsonify({'message': 'User added successfully.'}), 201

@app.route('/users', methods=['GET'])
def get_users():
    with duckdb.connect('bus.db') as con:
        result = con.execute('SELECT * FROM users').fetchall()

    users = [{
        'username': row[0], 
        'age': row[1], 
        'country': row[2]
        } for row in result]
    return jsonify(users)

@app.route('/users/delete_all', methods=['DELETE'])
def delete_all_users():
    with duckdb.connect('bus.db') as con:
        con.execute('DELETE FROM users')

    return jsonify({'message': 'All users have been deleted.'}), 200

@app.route('/user_stats', methods=['GET'])
def get_user_stats():
    with duckdb.connect('bus.db') as con:
        num_users = con.execute('SELECT COUNT(*) FROM users').fetchone()[0]
        avg_age = con.execute('SELECT AVG(age) FROM users').fetchone()[0]
        top_countries = con.execute('''
            SELECT country, COUNT(*) as count FROM users 
            GROUP BY country ORDER BY count DESC LIMIT 3
        ''').fetchall()

    return jsonify({
        'num_users': num_users,
        'avg_age': avg_age,
        'top_countries': [{
            'country': row[0], 'count': row[1]} for row in top_countries]
    })

@app.route('/date', methods=['GET'])
def get_date_data():
    try:
        df = load_data()
        column = 'Occurred_On'
        date = request.args.get('date')
        num = process_date_request(df, date, column)
        return jsonify(count=num)
    except Exception as e:
        return jsonify(error=str(e)), 500

@app.route('/reason', methods=['GET'])
def get_reason_data():
    try:
        df = load_data()
        column = 'Reason'
        reason = request.args.get('reason')
        num =  process_reason_request(df, reason, column)
        return jsonify(count=num)
    except Exception as e:
        return jsonify(error=str(e)), 500

@app.route('/boro', methods=['GET'])
def get_boro_data():
    try:
        df = load_data()
        column = 'Boro'
        boro = request.args.get('boro')
        num = process_boro_request(df, boro, column)
        return jsonify(count=num)
    except Exception as e:
        return jsonify(error=str(e)), 500
   
@app.route('/date_records', methods=['GET'])
def get_data_by_date():
    try:
        df = load_data()
        date = request.args.get('date')
        date = pd.to_datetime(date).date()
        result = df[df['Occurred_On'] == date]    
        if empty_check(result):
            return empty_check(result), 404    
        output_format = request.args.get('format', 'json')
        return format_output(result, output_format)
    except Exception as e:
        return jsonify(error=str(e)), 500
    
@app.route('/records', methods=['GET'])
def list_records():
    try:
        df = load_data()
        column = request.args.get('column')
        value = request.args.get('value')
        limit = int(request.args.get('limit', 10))
        offset = int(request.args.get('offset', 0))
        if column and value:
            result = df[df[column] == value]
        result = result.iloc[offset:offset + limit]
        if empty_check(result):
            return empty_check(result), 404    
        output_format = request.args.get('format', 'json')
        return format_output(result, output_format)
    except Exception as e:
        return jsonify(error=str(e)), 500

@app.route('/record/<int:id>', methods=['GET'])
def get_record_by_id(id):
    try:
        df = load_data()
        result = df[df['Busbreakdown_ID'] == id]
        if empty_check(result):
            return empty_check(result), 404    
        output_format = request.args.get('format', 'json')
        return format_output(result, output_format)
    except Exception as e:
        return jsonify(error=str(e)), 500

if __name__ == '__main__':
    app.run(debug=True)
