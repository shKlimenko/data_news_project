import json
import requests
from dotenv import load_dotenv
import os
from pyspark.sql import SparkSession
import pandas as pd

# spark = SparkSession.builder \
#     .appName("Test") \
#     .getOrCreate()


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


with open("latest_news.json", "w") as f:
      json.dump(data, f)

#df = spark.read.json("latest_news.json")

df = pd.read_json("latest_news.json")
print(df)

# for dt in data:
#     print('-----=====*****=====-----')
#     print(dt['title'])
#     print(dt['description'])
#     print('-------------------------')
#     print()

