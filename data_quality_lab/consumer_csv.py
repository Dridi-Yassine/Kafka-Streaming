from kafka import KafkaConsumer, KafkaProducer
import json
import time
import os
from collections import Counter
from datetime import datetime

TOPIC_IN = "transactions-dirty"
TOPIC_DLQ = "transactions-dlq"
BOOTSTRAP = "localhost:9092"
METRICS_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "metrics.log")

REQUIRED_FIELDS = ["transaction_id", "user_id", "amount", "timestamp"]

def now_iso() -> str:
    return datetime.utcnow().isoformat() + "Z"

def log_metrics(report: str):
    with open(METRICS_FILE, "a") as f:
        f.write(f"[{now_iso()}] {report}\n")

def validate_record(record: dict) -> tuple[bool, str | None, dict | None]:
    if not isinstance(record, dict):
        return False, "not_a_dict", None

    # Check required fields
    for f in REQUIRED_FIELDS:
        if f not in record or record[f] is None:
            return False, f"missing_field:{f}", None
        if isinstance(record[f], str) and record[f].strip() == "":
            return False, f"empty_field:{f}", None

    cleaned = dict(record)
    
    # Validation logic
    try:
        cleaned["amount"] = float(cleaned["amount"])
    except Exception:
        return False, "bad_type:amount", None

    ts = str(cleaned["timestamp"]).strip()
    try:
        # Attempt to normalize common formats
        datetime.fromisoformat(ts.replace("Z", "").replace(" ", "T").replace("/", "-"))
    except Exception:
        return False, "bad_format:timestamp", None

    if cleaned["amount"] <= 0:
        return False, "rule:non_positive_amount", None

    return True, None, cleaned

def dlq_payload(reason: str, raw_value, meta: dict) -> dict:
    return {
        "reason": reason,
        "raw": raw_value,
        "meta": meta,
        "ts": now_iso()
    }

def main():
    consumer = KafkaConsumer(
        TOPIC_IN,
        bootstrap_servers=BOOTSTRAP,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="dirty-data-consumer-v2",
        api_version=(2, 0, 2),
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        api_version=(2, 0, 2),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    total = 0
    valid = 0
    invalid = 0
    error_counts = Counter()

    t0 = time.time()
    last_print = time.time()
    PRINT_EVERY_SEC = 5

    print(f"[START] Validation Consumer: {TOPIC_IN} -> {TOPIC_DLQ}")

    for msg in consumer:
        total += 1
        meta = {"partition": msg.partition, "offset": msg.offset}

        try:
            record = msg.value
            is_ok, reason, cleaned = validate_record(record)

            if is_ok:
                valid += 1
                print(f"[OK] offset={msg.offset} tx={cleaned['transaction_id']} validated.")
            else:
                invalid += 1
                error_counts[reason] += 1
                producer.send(TOPIC_DLQ, value=dlq_payload(reason, record, meta))
                print(f"[INVALID] offset={msg.offset} reason={reason}")

        except Exception as e:
            invalid += 1
            reason = f"crash:{type(e).__name__}"
            error_counts[reason] += 1
            producer.send(TOPIC_DLQ, value=dlq_payload(reason, msg.value, meta))

        # KPI print and log
        now = time.time()
        if now - last_print >= PRINT_EVERY_SEC:
            elapsed = now - t0
            report = (f"KPI: total={total} valid={valid} invalid={invalid} "
                      f"rate={((invalid/total)*100 if total else 0):.2f}% "
                      f"top_errors={error_counts.most_common(2)}")
            print(f"\n[REPORT] {report}\n")
            log_metrics(report)
            last_print = now

if __name__ == "__main__":
    main()
