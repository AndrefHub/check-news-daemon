import asyncio
import aiopg
import aiohttp
import logging
from datetime import datetime, timedelta
from config import *

# Setup logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

MESSAGE = """На сайте {city} ({db}, {server}) нет новостей за последний день."""


async def send_notification(db):
    """Send a notification if no news are found in any database."""
    city = [city for city in DATABASES if city["db"] == db][0]
    async with aiohttp.ClientSession() as session:
        try:
            async with session.post(
                NOTIFY_URL,
                data={
                    "message": MESSAGE.format(
                        city=city["name"], db=city["db"], server=SERVER
                    ),
                    "password": NOTIFY_PASSWORD,
                },
            ) as resp:
                logging.info(f"Notification sent, response: {resp.status}")
        except Exception as e:
            logging.error(f"Error sending notification: {e}")


async def check_database(db):
    """Check if there are news added in the last 24 hours."""
    query = """
        SELECT COUNT(*) FROM "News" 
        WHERE "createdAt" >= NOW() - INTERVAL '24 hours';
    """
    try:
        async with aiopg.connect(dsn=DSN.format(dbname=db)) as conn:
            async with conn.cursor() as cur:
                await cur.execute(query)
                count = await cur.fetchone()
                return (db, count[0] if count else 0)
    except Exception as e:
        logging.error(f"Error checking database: {db} - {e}")
        return (db, -1)


async def main():
    """Main loop checking all databases periodically."""
    while True:
        logging.info("Checking databases for news...")
        tasks = [check_database(db["db"]) for db in DATABASES]
        results = await asyncio.gather(*tasks)

        ok = True
        for result in results:
            logging.info(f"{result[0]} - {result[1]} news found")
            if result[1] == 0:
                await send_notification(result[0])
                ok = False
        if ok:
            logging.info("News found, no notification needed.")
        else:
            logging.info("Some servers don't have news.")

        logging.info(f"Sleeping for {CHECK_INTERVAL} seconds...")
        await asyncio.sleep(CHECK_INTERVAL)


if __name__ == "__main__":
    asyncio.run(main())
