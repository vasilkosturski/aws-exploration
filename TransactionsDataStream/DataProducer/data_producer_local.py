import json
from kafka import KafkaProducer
from datetime import datetime, timedelta, timezone
from kafka.errors import KafkaError

KAFKA_TOPIC = 'transactions-input'
KAFKA_BROKER = 'localhost:29092'

start_time = datetime.now(timezone.utc)

def increment_time(minutes=0, seconds=0):
    return start_time + timedelta(minutes=minutes, seconds=seconds)


transactions = [
    # Normal transactions for different accounts
    {'accountId': 'acc1', 'amount': 50, 'eventTime': increment_time(minutes=15)},
    {'accountId': 'acc1', 'amount': 75, 'eventTime': increment_time(minutes=30)},
    {'accountId': 'acc2', 'amount': 20, 'eventTime': increment_time(minutes=45)},
    {'accountId': 'acc2', 'amount': 100, 'eventTime': increment_time(minutes=60)},
    {'accountId': 'acc3', 'amount': 65, 'eventTime': increment_time(minutes=75)},
    {'accountId': 'acc3', 'amount': 120, 'eventTime': increment_time(minutes=105)},
    {'accountId': 'acc4', 'amount': 30, 'eventTime': increment_time(minutes=120)},
    {'accountId': 'acc4', 'amount': 110, 'eventTime': increment_time(minutes=150)},

    # Non-fraudulent transaction pair with a big time gap for account 5
    {'accountId': 'acc5', 'amount': 5, 'eventTime': increment_time(minutes=180)},
    {'accountId': 'acc5', 'amount': 1000, 'eventTime': increment_time(minutes=195)},

    # Fraudulent transaction pair with a small time gap for account 6
    {'accountId': 'acc6', 'amount': 5, 'eventTime': increment_time(minutes=240)},
    {'accountId': 'acc6', 'amount': 1500, 'eventTime': increment_time(minutes=240, seconds=30)},

    # Additional normal transactions for more diversity
    {'accountId': 'acc7', 'amount': 45, 'eventTime': increment_time(minutes=250)},
    {'accountId': 'acc7', 'amount': 85, 'eventTime': increment_time(minutes=260)},

    # Additional potentially fraudulent transactions
    {'accountId': 'acc8', 'amount': 9, 'eventTime': increment_time(minutes=270)},
    {'accountId': 'acc8', 'amount': 2000, 'eventTime': increment_time(minutes=270, seconds=30)},
]

# Create Kafka producer without security settings
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def send_transaction(transaction):
    # Ensure the timestamp is in ISO-8601 format with a 'Z' at the end to indicate UTC time
    transaction['eventTime'] = transaction['eventTime'].isoformat().replace("+00:00", "Z")
    future = producer.send(KAFKA_TOPIC, value=transaction, key=transaction['accountId'].encode('utf-8'))
    try:
        result = future.get(timeout=10)
        print(f"Sent transaction for amount ${transaction['amount']} at {transaction['eventTime']}. "
              f"Topic: {result.topic}, Partition: {result.partition}, Offset: {result.offset}")
    except KafkaError as e:
        print(f"Failed to send transaction: {e}")

if __name__ == '__main__':
    for transaction in transactions:
        send_transaction(transaction)
    producer.flush()
