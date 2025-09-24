import asyncio
import json
import os
from text_embedder.aws_clients import get_aboto3_client, get_boto3_client
from text_embedder.config import ( METRICS_HOST,
                                   METRICS_PORT,
                                   OCR_OUTPUT_JSONL_SQS_QUEUE_NAME,
                                   MAX_MESSAGES,
                                   POLL_INTERVAL,
                                    CONCURRENCY)
from text_embedder.processor import process_file
from text_embedder.logger import get_logger
from text_embedder.metrics_server import start_metrics_server
from text_embedder.opensearch_client import ensure_index
from text_embedder.embedder import invoke_embedding_model

logger = get_logger("text_embedder.worker")

async def bootstrap_index():
    """
    On worker startup, create the OpenSearch index if it doesn't exist.
    Uses Bedrock to infer embedding dimension.
    """
    try:
        logger.info("Bootstrapping OpenSearch index...")
        dummy_vector = await invoke_embedding_model("bootstrap test")
        dim = len(dummy_vector)
        await ensure_index(dim)
        logger.info("Index bootstrap complete (dim=%s)", dim)
    except Exception as e:
        logger.exception("Index bootstrap failed: %s", e)
        raise

async def poll_loop():

    # Ensure index is ready before polling
    await bootstrap_index()

    while True:
        # client = await get_aboto3_client("sqs")
        # queue_response = client.get_queue_url(QueueName=OCR_OUTPUT_JSONL_SQS_QUEUE_NAME)
        # queue_url=queue_response["QueueUrl"]
        async with await get_aboto3_client("sqs") as sqs:
            queue_response = await sqs.get_queue_url(QueueName=OCR_OUTPUT_JSONL_SQS_QUEUE_NAME)
            queue_url = queue_response["QueueUrl"]
            resp = await sqs.receive_message(
                QueueUrl=queue_url,
                MaxNumberOfMessages=MAX_MESSAGES,
                WaitTimeSeconds=10,
                VisibilityTimeout=60
            )
            messages = resp.get("Messages", [])
            if not messages:
                await asyncio.sleep(POLL_INTERVAL)
                continue

            # process messages concurrently but limit concurrency
            sem = asyncio.Semaphore(CONCURRENCY)
            async def handle_message(msg):
                async with sem:
                    body = msg["Body"]
                    # assume body contains {"s3_key": "..."}
                    try:
                        payload = json.loads(body)
                        s3_key = payload["s3_key"]
                        receipt_handle = msg["ReceiptHandle"]
                        await process_file(s3_key)
                        # delete message
                        await sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)
                    except Exception as e:
                        logger.exception("Failed processing message: %s", e)
                        # let message visibility timeout expire so message goes to DLQ after max receives
            tasks = [asyncio.create_task(handle_message(m)) for m in messages]
            await asyncio.gather(*tasks, return_exceptions=True)

def start():
    start_metrics_server(host=os.environ.get("METRICS_HOST", "0.0.0.0"),
                         port=int(os.environ.get("METRICS_PORT", "8000")))
    logger.info("metrics server started",
                extra={"host": os.environ.get("METRICS_HOST"), "port": os.environ.get("METRICS_PORT")})

    logger.info("Worker starting with concurrency=%s", CONCURRENCY)
    loop = asyncio.get_event_loop()
    loop.run_until_complete(poll_loop())
#
# if __name__ == "__main__":
#     start()