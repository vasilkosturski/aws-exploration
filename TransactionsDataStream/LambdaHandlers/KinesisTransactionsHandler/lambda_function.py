import base64
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    for record in event['Records']:
        # Kinesis data is base64 encoded so decode here
        payload = base64.b64decode(record["kinesis"]["data"])
        logger.info('Decoded payload: %s', payload)
    return 'Successfully processed {} records.'.format(len(event['Records']))
