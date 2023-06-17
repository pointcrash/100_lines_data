import asyncio
import asyncpg
from binance.client import AsyncClient
from config import *

create_table_query = """
    CREATE TABLE IF NOT EXISTS eth_btc_table (
        id SERIAL PRIMARY KEY,
        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        exchange VARCHAR(50) NOT NULL,
        eth_price DECIMAL(18, 8) NOT NULL,
        btc_price DECIMAL(18, 8) NOT NULL
    )
"""

api_key = key
api_secret = secret


async def insert_price(conn, exchange, eth_price, btc_price):
    try:
        insert_query = "INSERT INTO eth_btc_table (timestamp, exchange, eth_price, btc_price) VALUES (CURRENT_TIMESTAMP, $1, $2, $3);"
        await conn.execute(insert_query, exchange, eth_price, btc_price, )

    except (Exception, asyncpg.Error) as error:
        print("Ошибка при вставке цены в таблицу 'eth_btc_table':", error)


async def fetch_ticker_binance(conn):
    client = await AsyncClient.create(api_key, api_secret)
    exchange = "Binance"

    while True:
        ticker_eth = await client.futures_ticker(symbol='ETHUSDT')
        ticker_btc = await client.futures_ticker(symbol='BTCUSDT')

        eth_price = float(ticker_eth['lastPrice'])
        btc_price = float(ticker_btc['lastPrice'])

        await insert_price(conn, exchange, eth_price, btc_price)


async def main():
    conn = await asyncpg.connect(dsn=f"postgres://{username}:{password}@{host}:{port}/{database_name}")
    await conn.execute(create_table_query)
    await fetch_ticker_binance(conn)
    await conn.close()


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
