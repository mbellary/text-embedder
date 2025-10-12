import json
import io
import asyncio
from text_embedder.aws_clients import get_aboto3_client
from text_embedder.config import CHUNK_PAGE_STATE_NAME, PAGE_CHUNK_SQS_QUEUE_NAME, CHUNK_S3_BUCKET
from text_embedder.logger import get_logger

logger = get_logger("text_embedder.utils")

async def fetch_s3_jsonl(s3_key: str):
    """
    Return generator/list of JSON objects from S3 JSONL file.
    """
    client =  await get_aboto3_client("s3")
    async with client as s3:
        resp = await s3.get_object(Bucket=CHUNK_S3_BUCKET, Key=s3_key)
        body = await resp['Body'].read()
        lines = body.decode('utf-8').splitlines()
        return [json.loads(l) for l in lines if l.strip()]
        # ocr_content = [json.loads(l) for l in lines if l.strip()]
        # for content in ocr_content:
        #     pages = content['pages']
        # return pages

async def delete_sqs_message(receipt_handle: str):
    client =  await get_aboto3_client("sqs")
    queue_response = await client.get_queue_url(QueueName=PAGE_CHUNK_SQS_QUEUE_NAME)
    queue_url = queue_response['QueueUrl']
    async with client as sqs:
        await sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)

async def change_dynamodb_status(file_key: str, status: str, extra: dict = None):
    client =  await get_aboto3_client("dynamodb")
    item = {
        's3_key': {'S': file_key},
        'status': {'S': status}
    }
    if extra:
        for k, v in extra.items():
            item[k] = {'S': str(v)}
    async with client as ddb:
        await ddb.put_item(TableName=CHUNK_PAGE_STATE_NAME, Item=item)
