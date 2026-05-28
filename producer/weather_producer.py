import asyncio
import json
import os

import aiohttp
from aiokafka import AIOKafkaProducer


async def fetch_district(
    session: aiohttp.ClientSession, base_url: str, district: dict
) -> dict | None:
    timeout = aiohttp.ClientTimeout(total=5)
    try:
        async with session.get(
            f"{base_url}/events/weather_district",
            params={"district": district["district"], "city": district["city"]},
            timeout=timeout,
        ) as r:
            r.raise_for_status()
            return await r.json()
    except Exception as e:
        print(f"  weather error {district['district']}: {e}")
        return None


async def main():
    base_url = os.getenv("API_BASE_URL", "http://traffic-api:8000")
    kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP", "kafka:29092")
    kafka_topic = os.getenv("KAFKA_TOPIC", "trafic.raw_weather")
    batch_interval = float(os.getenv("POLL_INTERVAL_MS", "10000")) / 1000

    async with aiohttp.ClientSession() as session:
        async with session.get(
            f"{base_url}/roads", timeout=aiohttp.ClientTimeout(total=10)
        ) as r:
            routes = await r.json()

    if not routes:
        raise RuntimeError("No routes returned from traffic API")

    seen: set[tuple[str, str]] = set()
    districts: list[dict] = []
    for r in routes:
        key = (r["district"], r["city"])
        if key not in seen:
            seen.add(key)
            districts.append({"district": r["district"], "city": r["city"]})

    kafka = AIOKafkaProducer(bootstrap_servers=kafka_bootstrap)
    await kafka.start()
    print(f"Weather producer: {len(districts)} districts, batch_interval={batch_interval}s")

    try:
        tick = 0
        while True:
            t0 = asyncio.get_event_loop().time()
            async with aiohttp.ClientSession() as session:
                events = await asyncio.gather(
                    *[fetch_district(session, base_url, d) for d in districts]
                )
            sent = 0
            for event in events:
                if event is not None:
                    await kafka.send(kafka_topic, json.dumps(event, ensure_ascii=False).encode())
                    sent += 1
            await kafka.flush()
            elapsed = asyncio.get_event_loop().time() - t0
            print(f"[{tick}] weather sent {sent}/{len(districts)} in {elapsed:.2f}s")
            tick += 1
            await asyncio.sleep(max(0.0, batch_interval - elapsed))
    finally:
        await kafka.stop()


if __name__ == "__main__":
    asyncio.run(main())
