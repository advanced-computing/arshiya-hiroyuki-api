from flask import Flask, jsonify, render_template, request
import pandas as pd 
from flask import render_template

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

df=pd.read_csv("Bus_Breakdown_and_Delays_20250214.csv", low_memory=False)
df = pd.DataFrame(df)

@app.route("/")
def index():
    return "Hello, Welcome to our app!"

