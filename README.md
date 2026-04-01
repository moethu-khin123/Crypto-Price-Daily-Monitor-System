# Crypto Price Monitor Pipeline 🚀

## 📌 Project Overview
This project is an automated data pipeline using Apache Airflow to:
- Fetch cryptocurrency prices (Bitcoin, Ethereum)
- Store data in database
- Send Telegram alerts

## 🛠️ Tech Stack
- Python
- Apache Airflow
- Docker
- PostgreSQL
- Telegram Bot API

## ⚙️ Features
- Hourly scheduled pipeline
- API data ingestion (CoinGecko)
- Database storage
- Telegram notification system

## ⚙️ Setup Instructions
1. Clone the repository
git clone https://github.com/your-username/crypto-price-monitor.git
cd crypto-price-monitor
2. Install Docker
Make sure Docker and Docker Compose are installed:
docker --version
docker-compose --version
3. Install Python dependencies (optional for local testing)
pip install -r requirements.txt
4. Configure Environment
Set Airflow Variables (via UI or CLI):
telegram_bot_token
telegram_chat_id

## 🚀 How to Run
1. Start all services
docker-compose up -d
2. Open Airflow UI
Go to:
http://localhost:8080
3. Enable DAGs
Turn on:
crawl_crypto_prices (hourly data collection)
daily_crypto_report (daily report)

## 📩 Notification
Telegram bot sends alert when pipeline runs successfully
