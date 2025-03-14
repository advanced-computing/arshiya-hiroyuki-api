import requests

url = 'http://127.0.0.1:5000/users/delete_all'
response = requests.delete(url)

if response.status_code == 200:
    print("All users have been deleted successfully!")
else:
    print(f"Failed to delete users: {response.json()}")