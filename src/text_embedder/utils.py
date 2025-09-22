import json
import io
import asyncio
from text_embedder.aws_clients import get_boto3_client
from text_embedder.config import settings
from text_embedder.logger import get_logger

logger = get_logger("text_embedder.utils")

async def fetch_s3_jsonl(s3_key: str):
    """
    Return generator/list of JSON objects from S3 JSONL file.
    """
    client = await get_boto3_client("s3")
    async with client as s3:
        resp = await s3.get_object(Bucket=settings.s3_bucket, Key=s3_key)
        body = await resp['Body'].read()
        lines = body.decode('utf-8').splitlines()
        return [json.loads(l) for l in lines if l.strip()]

async def delete_sqs_message(receipt_handle: str):
    client = await get_boto3_client("sqs")
    async with client as sqs:
        await sqs.delete_message(QueueUrl=settings.sqs_queue_url, ReceiptHandle=receipt_handle)

async def change_dynamodb_status(file_key: str, status: str, extra: dict = None):
    client = await get_boto3_client("dynamodb")
    item = {
        'file_key': {'S': file_key},
        'status': {'S': status}
    }
    if extra:
        for k, v in extra.items():
            item[k] = {'S': str(v)}
    async with client as ddb:
        await ddb.put_item(TableName=settings.dynamodb_table, Item=item)
