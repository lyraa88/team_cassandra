import os, time, csv, sys
from kafka import KafkaProducer
from dotenv import load_dotenv

load_dotenv()

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "logs_raw")
CSV_PATH = os.getenv("CSV_PATH", "/app/ingest/logs.csv")
RATE = float(os.getenv("SEND_RATE_PER_SEC", "50"))

def main():
    producer = KafkaProducer(bootstrap_servers=BOOTSTRAP, acks='all')
    sent = 0
    delay = 1.0 / RATE if RATE > 0 else 0.0

    if not os.path.isfile(CSV_PATH):
        print(f"[ERROR] CSV not found: {CSV_PATH}", file=sys.stderr)
        sys.exit(1)

    with open(CSV_PATH, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            # 간단히 CSV 행을 원문 JSON-like 문자열로
            payload = str(row).encode("utf-8")
            producer.send(TOPIC, payload)
            sent += 1
            if delay > 0:
                time.sleep(delay)
            if sent % 100 == 0:
                print(f"sent={sent}", flush=True)

    producer.flush()
    print(f"Done. total sent={sent}")

if __name__ == "__main__":
    main()

