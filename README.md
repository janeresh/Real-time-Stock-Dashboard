# Real Time Stock Dashboard

## Setup Postgres
### 1. Set the Environment variables
```
POSTGRES_DB=stock_db
POSTGRES_USER=stocks
POSTGRES_PASSWORD=stocks
POSTGRES_PORT=5432
POSTGRES_HOST=localhost
TICKERS=AAPL,MSFT,GOOGL,AMZN,NVDA
POLL_SECONDS=5
```
### 2. Start Postgres
```
docker compose -f docker/docker-compose.yml --env-file .env up -d
```

### 3. You can verify if it is working
```
docker ps 
# should show a postgres container

docker exec -it <postgres_container> env PGPASSWORD='pass' pg_isready -U user -d stock_db
# Expected output - localhost:5432 - accepting connections
```

## Download the requirements
### 1. (Optional) Start venv
```
python -m venv .venv
source .venv/bin/activate
```

### 2. Download requirements
```
pip install -r requirements.txt
```

## Start Producer
Run this in new terminal. Make sure venv is initialised in this terminal.
```
python producer.py
```

## Start Consumer
Run this in new terminal. Make sure venv is initialised in this terminal.
```
python consumer.py
```

## Run Streamlit dashboard
Run this in new terminal. Make sure venv is initialised in this terminal.
```
python -m streamlit run app.py
```