import requests
from dotenv import load_dotenv
import os

load_dotenv()

API_KEY = os.getenv("API_KEY")

url = ('https://newsapi.org/v2/top-headlines?'
       'country=us&'
       #'from=2025-05-31&'
       #'to=2025-05-31&'
       #'sortBy=date&'
       f'apiKey={API_KEY}')

response = requests.get(url)
data = response.json()['articles']

for dt in data:
    print('-----=====*****=====-----')
    print(dt['title'])
    print(dt['description'])
    print('-------------------------')
    print()

