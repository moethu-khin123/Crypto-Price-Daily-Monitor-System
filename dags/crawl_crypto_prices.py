from datetime import datetime, timedelta
from datetime import datetime as dt
import sys
import requests
import logging
import time

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

sys.path.append("/opt/airflow")

from scripts.db import insert_prices


# ✅ Logger
logger = logging.getLogger(__name__)


# ✅ TASK 1: Fetch + Store (UPDATED)
def fetch_and_store_crypto_prices(**context):
    """
    Fetch crypto prices with robust error handling & retry logic
    """
    url = "https://api.coingecko.com/api/v3/simple/price"
    params = {
        "ids": "bitcoin,ethereum",
        "vs_currencies": "usd",
        "include_last_updated_at": "true"
    }

    logger.info(f"🚀 Starting crypto crawl at {dt.utcnow()}")

    max_retries = 3
    backoff = 5  # seconds

    for attempt in range(1, max_retries + 1):
        try:
            logger.info(f"🔄 API attempt {attempt}")

            response = requests.get(url, params=params, timeout=10)

            # ✅ Handle rate limit
            if response.status_code == 429:
                logger.warning("⚠️ Rate limit hit (429)")
                raise Exception("Rate limit")

            # ✅ Handle server errors
            if 500 <= response.status_code < 600:
                logger.warning(f"⚠️ Server error {response.status_code}")
                raise Exception("Server error")

            response.raise_for_status()

            data = response.json()
            rows = []

            for coin_id in ["bitcoin", "ethereum"]:
                price = data[coin_id]["usd"]
                last_updated = dt.utcfromtimestamp(
                    data[coin_id]["last_updated_at"]
                )

                rows.append({
                    "coin_id": coin_id,
                    "price_usd": price,
                    "source_last_updated_at": last_updated
                })

                logger.info(f"💰 {coin_id.upper()}: ${price:,.2f}")

            # ✅ Insert into DB
            insert_prices(rows)

            logger.info(f"✅ Inserted {len(rows)} records successfully")

            # ✅ Pass data to next task
            context['ti'].xcom_push(key='crypto_prices', value=rows)

            return {
                "status": "success",
                "records_inserted": len(rows)
            }

        except requests.exceptions.Timeout:
            logger.error(f"❌ Timeout on attempt {attempt}")

        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Request error on attempt {attempt}: {e}")

        except Exception as e:
            logger.error(f"❌ Attempt {attempt} failed: {e}")

        # 🔁 Retry with backoff
        if attempt < max_retries:
            sleep_time = backoff * attempt
            logger.info(f"⏳ Retrying in {sleep_time} seconds...")
            time.sleep(sleep_time)
        else:
            logger.error("❌ All retry attempts failed")
            raise


# ✅ TASK 2: Telegram Alert
def send_telegram_message(**context):
    try:
        token = Variable.get("telegram_bot_token", default_var=None)
        chat_id = Variable.get("telegram_chat_id", default_var=None)

        if not token or not chat_id:
            logger.warning("⚠️ Telegram not configured, skipping alert")
            return

        token = token.strip()
        chat_id = chat_id.strip()

        execution_time = context['execution_date'].strftime('%Y-%m-%d %H:%M UTC')

        prices = context['ti'].xcom_pull(
            task_ids='fetch_and_store_prices',
            key='crypto_prices'
        )

        if prices:
            btc_price = next((p['price_usd'] for p in prices if p['coin_id'] == 'bitcoin'), None)
            eth_price = next((p['price_usd'] for p in prices if p['coin_id'] == 'ethereum'), None)

            message = f"""🤖 Crypto Pipeline Report
📅 Time: {execution_time}
✅ Status: Success

💰 Prices:
- Bitcoin: ${btc_price:,.2f}
- Ethereum: ${eth_price:,.2f}
"""
        else:
            message = f"✅ Pipeline ran successfully at {execution_time}"

        url = f"https://api.telegram.org/bot{token}/sendMessage"

        payload = {
            "chat_id": chat_id,
            "text": message
        }

        response = requests.post(url, data=payload, timeout=10)

        logger.info(f"📨 Telegram response: {response.text}")

        if response.status_code != 200:
            raise Exception(f"Telegram API error: {response.text}")

        logger.info("✅ Telegram message sent successfully")

    except Exception as e:
        logger.error(f"❌ Telegram error: {e}")
        # Do NOT fail DAG if Telegram fails


# ✅ DAG
with DAG(
    dag_id="crawl_crypto_prices",
    start_date=datetime(2024, 1, 1),
    schedule="@hourly",
    catchup=False,
    max_active_runs=1,

    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=2),
    },

    tags=["crypto", "pipeline"],
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