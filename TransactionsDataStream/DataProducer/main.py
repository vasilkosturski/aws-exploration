import json
import boto3
from datetime import datetime, timedelta

STREAM_NAME = 'TransactionsInputDataStream'

kinesis_client = boto3.client('kinesis', region_name='us-east-1')

start_time = datetime.now()


def increment_time(minutes=0, seconds=0):
    return start_time + timedelta(minutes=minutes, seconds=seconds)


transactions = [
    # Normal transactions
    {'accountId': 'acc123', 'amount': 50, 'eventTime': increment_time(minutes=15)},
    {'accountId': 'acc123', 'amount': 75, 'eventTime': increment_time(minutes=30)},
    {'accountId': 'acc123', 'amount': 20, 'eventTime': increment_time(minutes=45)},
    {'accountId': 'acc123', 'amount': 100, 'eventTime': increment_time(minutes=60)},
    {'accountId': 'acc123', 'amount': 65, 'eventTime': increment_time(minutes=75)},
    {'accountId': 'acc123', 'amount': 120, 'eventTime': increment_time(minutes=105)},
    {'accountId': 'acc123', 'amount': 30, 'eventTime': increment_time(minutes=120)},
    {'accountId': 'acc123', 'amount': 110, 'eventTime': increment_time(minutes=150)},
    # Non-fraudulent transaction pair with a big time gap
    {'accountId': 'acc123', 'amount': 5, 'eventTime': increment_time(minutes=180)},
    {'accountId': 'acc123', 'amount': 1000, 'eventTime': increment_time(minutes=195)},
    # Fraudulent transaction pair with a small time gap
    {'accountId': 'acc123', 'amount': 5, 'eventTime': increment_time(minutes=240)},
    {'accountId': 'acc123', 'amount': 1500, 'eventTime': increment_time(minutes=240, seconds=30)},
]


def send_transaction(transaction):
    transaction['eventTime'] = transaction['eventTime'].isoformat()
    result = kinesis_client.put_record(
        StreamName=STREAM_NAME,
        Data=json.dumps(transaction),
        PartitionKey=transaction['accountId']
    )
    print(f"Sent transaction for amount ${transaction['amount']} at {transaction['eventTime']}. "
          f"Sequence number: {result['SequenceNumber']}")


if __name__ == '__main__':
    for transaction in transactions:
        send_transaction(transaction)
