import subprocess
import time
import sys
import os

# Add path
sys.path.insert(0, r'C:\Users\Lester\repositories\github\enterprise\FastBI-EMR-Glue-Platform')

# Start server in background
proc = subprocess.Popen(
    [r'C:\Users\Lester\repositories\github\enterprise\FastBI-EMR-Glue-Platform\venv\Scripts\python.exe', '-m', 'uvicorn', 'main:app', '--host', '0.0.0.0', '--port', '7777', '--reload'],
    cwd=r'C:\Users\Lester\repositories\github\enterprise\FastBI-EMR-Glue-Platform',
    stdout=subprocess.PIPE,
    stderr=subprocess.PIPE
)

# Wait for server to start
time.sleep(3)

import urllib.request
import json

# Test with query parameters
url = 'http://localhost:7777/auth/token?client_id=jholgJ1hEcb5Fty2&client_secret=w1VvvSax1vUcED6s'
try:
    req = urllib.request.Request(url, method='POST')
    with urllib.request.urlopen(req) as response:
        print(f"Status: {response.status}")
        print(f"Body: {response.read().decode()}")
except urllib.error.HTTPError as e:
    print(f"Error status: {e.code}")
    print(f"Error body: {e.read().decode()}")

# Test with form data in body
url2 = 'http://localhost:7777/auth/token'
try:
    data = 'client_id=jholgJ1hEcb5Fty2&client_secret=w1VvvSax1vUcED6s'.encode()
    req2 = urllib.request.Request(url2, method='POST', data=data)
    with urllib.request.urlopen(req2) as response:
        print(f"Form body status: {response.status}")
        print(f"Form body response: {response.read().decode()}")
except urllib.error.HTTPError as e:
    print(f"Form body error status: {e.code}")
    print(f"Form body error body: {e.read().decode()}")

# Kill the server
proc.terminate()
proc.wait()
print("Server stopped")