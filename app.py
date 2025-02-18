from flask import Flask, jsonify, request
import pandas as pd

app = Flask(__name__)

@app.route("/")
def echo():
    print("-----START-----")
    print("Method:", request.method)
    print("URL:", request.url)
    print("Headers:\n")
    print(request.headers)
    print(f'Body: "{request.get_data(as_text=True)}"')
    print("-----END-----")
    return "see console"

df = pd.read_csv("bus_2024_2025.csv")
df['Occurred_On'] = pd.to_datetime(df['Occurred_On'], format='%m/%d/%Y %I:%M:%S %p')

@app.route("/date", methods=["GET"])
def get_date_data():
    date = request.args.get("date")
    date = pd.to_datetime(date).date()
    num = df[df['Occurred_On'].dt.date == date].shape[0]
    return jsonify(count=num)

@app.route("/reason", methods=["GET"])
def get_reason_data():
    reason = request.args.get("reason")
    num = df[df['Reason'] == reason].shape[0]
    return jsonify(count=num)

@app.route("/boro", methods=["GET"])
def get_boro_data():
    boro = request.args.get("boro")
    num = df[df['Boro'] == boro].shape[0]
    return jsonify(count=num)

@app.route("/date_records", methods=["GET"])
def get_data_by_date():
    date = request.args.get("date")
    output_format = request.args.get("format", "json") 
    try:
        date = pd.to_datetime(date).date()
        result = df[df['Occurred_On'].dt.date == date]
        if result.empty:
            return jsonify(message="No Records"), 404
        if output_format == "csv":
            return result.to_csv(index=False)
        else:
            return result.to_json(orient="records")
    except Exception as e:
        return jsonify(error=str(e)), 400
    
@app.route("/records", methods=["GET"])
def list_records():
    output_format = request.args.get("format", "json")
    column = request.args.get("column")
    value = request.args.get("value")
    limit = int(request.args.get("limit", 10))
    offset = int(request.args.get("offset", 0))

    filtered_df = df
    if column and value:
        filtered_df = filtered_df[filtered_df[column] == value]

    filtered_df = filtered_df.iloc[offset:offset + limit]

    if output_format == "csv":
        return filtered_df.to_csv(index=False)
    else:
        return filtered_df.to_json(orient="records")

@app.route("/record/<int:id>", methods=["GET"])
def get_record_by_id(id):
    record = df[df["Busbreakdown_ID"] == id]
    if record.empty:
        return jsonify(error="Record not found"), 404
    else:
        output_format = request.args.get("format", "json") 
        if output_format == "csv":
            return record.to_csv(index=False)
        else:
            return record.to_json(orient="records")

if __name__ == "__main__":
    app.run(debug=True)