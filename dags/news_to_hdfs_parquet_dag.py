import os
import json
import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from hdfs import InsecureClient
from pyspark.sql import SparkSession
from dotenv import load_dotenv
import requests

# Настройки
load_dotenv()
NEWS_API_KEY = os.getenv("NEWS_API_KEY")
HDFS_NAMENODE = "http://45.156.21.217:9000"
HDFS_RAW_DIR = "/user/news/raw"
HDFS_CLEAN_DIR = "/user/news/parquet"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 6, 5),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Инициализация клиента HDFS
client = InsecureClient(HDFS_NAMENODE)

def fetch_news(**kwargs):
    """Забирает новости из API и сохраняет их в HDFS как JSON"""
    url = f'https://newsapi.org/v2/top-headlines?country=us&apiKey={NEWS_API_KEY}'
    response = requests.get(url)
    data = response.json().get('articles', [])

    if not data:
        logging.info("Новостей нет.")
        return None

    # Добавляем временную метку 
    now = datetime.now()
    filename = f"news_{now.strftime('%Y%m%d_%H%M%S')}.json"

    local_path = f"/tmp/{filename}"
    with open(local_path, "w", encoding="utf-8") as f:
        json.dump(data, f)

    # Сохраняем в HDFS сырые данные
    hdfs_path = f"{HDFS_RAW_DIR}/{filename}"
    client.upload(hdfs_path, local_path)
    logging.info(f"Сырые данные сохранены в HDFS: {hdfs_path}")

    return hdfs_path


def deduplicate_and_save_parquet(**kwargs):
    """Читает JSON из HDFS, убирает дубли и сохраняет как Parquet"""

    ti = kwargs['ti']
    raw_hdfs_path = ti.xcom_pull(task_ids='fetch_news')

    if not raw_hdfs_path:
        logging.info("Нет данных для обработки.")
        return

    # Локальный путь для временного файла
    tmp_json = "/tmp/temp_news.json"
    client.download(raw_hdfs_path, tmp_json)

    with open(tmp_json, "r", encoding="utf-8") as f:
        articles = json.load(f)

    # Уникальность по URL
    seen_urls = set()
    unique_articles = []

    for article in articles:
        url = article.get("url")
        if url and url not in seen_urls:
            seen_urls.add(url)
            unique_articles.append(article)

    if not unique_articles:
        logging.info("Все статьи — дубликаты.")
        return

    # Сохраняем уникальные данные временно в локальный JSON
    tmp_cleaned = "/tmp/cleaned_news.json"
    with open(tmp_cleaned, "w", encoding="utf-8") as f:
        json.dump(unique_articles, f)

    # Инициализируем Spark
    spark = SparkSession.builder \
        .appName("NewsToParquet") \
        .getOrCreate()

    df = spark.read.json(tmp_cleaned)
    now = datetime.now()
    hdfs_parquet_path = f"{HDFS_CLEAN_DIR}/year={now.year}/month={now.month}/day={now.day}"

    df.write.mode("append").parquet(hdfs_parquet_path)
    spark.stop()

    logging.info(f"Данные записаны в Parquet: {hdfs_parquet_path}")


with DAG(
    dag_id='news_to_hdfs_parquet',
    default_args=default_args,
    schedule_interval='@hourly',
    catchup=False,
) as dag:

    fetch_task = PythonOperator(
        task_id='fetch_news',
        python_callable=fetch_news,
        provide_context=True
    )

    process_task = PythonOperator(
        task_id='deduplicate_and_save_parquet',
        python_callable=deduplicate_and_save_parquet,
        provide_context=True
    )

    fetch_task >> process_task