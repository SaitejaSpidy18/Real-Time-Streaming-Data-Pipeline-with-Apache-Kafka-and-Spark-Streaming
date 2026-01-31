import json
import time
import random
from datetime import datetime, timedelta, timezone

from kafka import KafkaProducer

KAFKA_BOOTSTRAP_SERVERS = "localhost:29092"
TOPIC = "user_activity"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

pages = ["/", "/home", "/product/1", "/product/2", "/cart", "/checkout"]
event_types = ["page_view", "click", "session_start", "session_end"]

def random_event(late=False):
    now = datetime.now(timezone.utc)
    if late:
        event_time = now - timedelta(minutes=3)
    else:
        event_time = now
    return {
        "event_time": event_time.isoformat(),
        "user_id": f"user_{random.randint(1, 5)}",
        "page_url": random.choice(pages),
        "event_type": random.choice(event_types),
    }

if __name__ == "__main__":
    print(f"Producing events to topic {TOPIC}...")
    i = 0
    try:
        while True:
            late = (i % 20 == 0 and i > 0)
            evt = random_event(late=late)
            producer.send(TOPIC, evt)
            print("Sent:", evt)
            i += 1
            time.sleep(1)
    except KeyboardInterrupt:
        print("Stopping producer...")
    finally:
        producer.flush()
        producer.close()
