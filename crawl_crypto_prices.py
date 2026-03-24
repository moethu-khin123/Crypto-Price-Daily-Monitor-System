from datetime import datetime
from datetime import datetime as dt
import sys

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import requests

sys.path.append("/opt/airflow")

from scripts.db import insert_prices


def fetch_and_store_crypto_prices():
    url = "https://api.coingecko.com/api/v3/simple/price"
    params = {
        "ids": "bitcoin,ethereum",
        "vs_currencies": "usd",
        "include_last_updated_at": "true"
    }

    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()
    data = response.json()

    rows = []
    for coin_id in ["bitcoin", "ethereum"]:
        rows.append({
            "coin_id": coin_id,
            "price_usd": data[coin_id]["usd"],
            "source_last_updated_at": dt.utcfromtimestamp(
                data[coin_id]["last_updated_at"]
            )
        })

    insert_prices(rows)
    print(rows)


def send_telegram_message():
    try:
        # Get variables from Airflow
        token = Variable.get("telegram_bot_token")
        chat_id = Variable.get("telegram_chat_id")
        
        # Clean and validate
        token = token.strip() if token else ""
        chat_id = chat_id.strip() if chat_id else ""
        
        print(f"Token: {token[:20]}...")
        print(f"Chat ID: {chat_id}")
        
        if not token:
            raise ValueError("Telegram bot token is empty!")
        if not chat_id:
            raise ValueError("Telegram chat ID is empty!")
        
        message = "✅ Crypto pipeline is running successfully!"
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        
        payload = {
            "chat_id": chat_id,
            "text": message
        }
        
        response = requests.post(url, data=payload, timeout=10)
        print(f"Response Status: {response.status_code}")
        print(f"Response: {response.text}")
        response.raise_for_status()
        
    except Exception as e:
        print(f"ERROR: {e}")
        raise


with DAG(
    dag_id="crawl_crypto_prices",
    start_date=datetime(2024, 1, 1),
    schedule="@hourly",
    catchup=False,
) as dag:

    task_fetch_and_store = PythonOperator(
        task_id="fetch_and_store_prices",
        python_callable=fetch_and_store_crypto_prices,
    )

    send_alert = PythonOperator(
        task_id="send_telegram_alert",
        python_callable=send_telegram_message,
    )

    task_fetch_and_store >> send_alert