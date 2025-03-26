import requests
import json

url = "https://api.powerbi.com/beta/...your_push_url..."
headers = {'Content-Type': 'application/json'}

data = {
  "symbol": "AAPL",
  "price": 159.44,
  "timestamp": "2025-03-24T10:00:00"
}

requests.post(url, headers=headers, data=json.dumps(data))
