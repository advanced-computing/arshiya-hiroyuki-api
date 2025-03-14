import requests

url = 'http://127.0.0.1:5000/users'

users = [
    {'username': 'Alice', 'age': 30, 'country': 'USA'},
    {'username': 'Bob', 'age': 25, 'country': 'USA'},
    {'username': 'Charlie', 'age': 35, 'country': 'Canada'},
    {'username': 'David', 'age': 28, 'country': 'France'},
    {'username': 'Eve', 'age': 22, 'country': 'France'},
    {'username': 'Frank', 'age': 40, 'country': 'Germany'},
    {'username': 'Grace', 'age': 29, 'country': 'Canada'}
]

for user in users:
    response = requests.post(url, json=user)
    if response.status_code == 201:
        print(f"User {user['username']} added successfully!")
    else:
        print(f"Failed to add {user['username']}: {response.json()}")

