import json
import boto3
from datetime import datetime, timedelta

STREAM_NAME = 'TransactionsInputStream'

kinesis_client = boto3.client('kinesis', region_name='us-east-1')

start_time = datetime.now()


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
