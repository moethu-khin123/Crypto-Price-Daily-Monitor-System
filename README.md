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

## 🚀 How to Run
1. Start Airflow:
   docker-compose up

2. Trigger DAG:
   airflow dags trigger crawl_crypto_prices

## 📩 Notification
Telegram bot sends alert when pipeline runs successfully