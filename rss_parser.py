import feedparser
import pandas as pd
from datetime import datetime

#url = ('https://tass.ru/rss/v2.xml')

def rss_to_dataframe(url):
    feed = feedparser.parse(url)
    articles = []
    
    for entry in feed.entries:
        articles.append({
            'title': entry.title,
            'link': entry.link,
            'published': datetime(*entry.published_parsed[:6]),
            'category': entry.category
        })
    
    return pd.DataFrame(articles)

# Пример использования
pd.set_option('display.max_colwidth', None)
df = rss_to_dataframe("https://lenta.ru/rss/news")
print(df.head(50))