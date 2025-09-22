from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Any, Dict
import asyncio
from text_embedder.opensearch_client import index_document, search, delete_document, vector_search, index_exists
from text_embedder.logger import get_logger
from text_embedder.embedder import invoke_bedrock_embedding
from text_embedder.config import (
                                    OPENSEARCH_HOST,
                                    OPENSEARCH_INDEX,
                                    OCR_S3_BUCKET,
                                    OCR_JSONL_SQS_QUEUE_NAME,
                                    EMBEDDER_PAGE_STATE_NAME
)
from text_embedder.aws_clients import get_boto3_client

logger = get_logger("text_embedder.api")
app = FastAPI(title="text-embedder-api")

class IndexPayload(BaseModel):
    doc_id: str
    document: Dict[str, Any]

@app.get("/health")
async def healthcheck():
    """
    Full system health check (soft mode).
    Always returns HTTP 200 so ALB doesn't kill tasks,
    but reports "degraded" in JSON if any dependency fails.
    """
    checks = {}

    # --- OpenSearch ---
    try:
        exists = await index_exists()
        checks["opensearch"] = "ok" if exists else "missing-index"
    except Exception as e:
        logger.exception("OpenSearch healthcheck failed")
        checks["opensearch"] = f"error: {str(e)}"

    # --- S3 ---
    try:
        client = await get_boto3_client("s3")
        async with client as s3:
            await s3.head_bucket(Bucket=OCR_S3_BUCKET)
        checks["s3"] = "ok"
    except Exception as e:
        logger.exception("S3 healthcheck failed")
        checks["s3"] = f"error: {str(e)}"

    # --- SQS ---
    try:
        client = await get_boto3_client("sqs")
        queue_response = client.get_queue_url(QueueName=OCR_JSONL_SQS_QUEUE_NAME)
        queue_url = queue_response["QueueUrl"]
        async with client as sqs:
            await sqs.get_queue_attributes(
                QueueUrl=queue_url,
                AttributeNames=["ApproximateNumberOfMessages"]
            )
        checks["sqs"] = "ok"
    except Exception as e:
        logger.exception("SQS healthcheck failed")
        checks["sqs"] = f"error: {str(e)}"

    # --- DynamoDB ---
    try:
        client = await get_boto3_client("dynamodb")
        async with client as ddb:
            await ddb.describe_table(TableName=EMBEDDER_PAGE_STATE_NAME)
        checks["dynamodb"] = "ok"
    except Exception as e:
        logger.exception("DynamoDB healthcheck failed")
        checks["dynamodb"] = f"error: {str(e)}"

    # --- Bedrock ---
    try:
        # Minimal test: embed a short dummy string
        vector = await invoke_bedrock_embedding("healthcheck")
        if isinstance(vector, list) and len(vector) > 0:
            checks["bedrock"] = f"ok (dim={len(vector)})"
        else:
            checks["bedrock"] = "unexpected-response"
    except Exception as e:
        logger.exception("Bedrock healthcheck failed")
        checks["bedrock"] = f"error: {str(e)}"

    # Final status
    all_ok = all(v == "ok" or v.startswith("ok") for v in checks.values())
    status = "ok" if all_ok else "degraded"

    return {"status": status, "checks": checks}

@app.post("/index")
async def create_index_item(body: IndexPayload):
    try:
        result = await index_document(body.doc_id, body.document)
        return {"ok": True, "result": result}
    except Exception as e:
        logger.exception("Index error")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/search")
async def do_search(q: str, size: int = 10):
    # simple match query — you can also build vector kNN query depending on your OpenSearch version
    query = {"query": {"multi_match": {"query": q, "fields": ["text", "metadata.*"]}}, "size": size}
    try:
        res = await search(query, size=size)
        return res
    except Exception as e:
        logger.exception("Search error")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/semantic-search")
async def semantic_search(query: str, k: int = 5):
    # embed the query with Bedrock
    vector = await invoke_bedrock_embedding(query)

    res = await vector_search(vector, k=k)
    return res

@app.delete("/doc/{doc_id}")
async def delete_doc(doc_id: str):
    try:
        res = await delete_document(doc_id)
        return {"ok": True, "result": res}
    except Exception as e:
        logger.exception("Delete error")
        raise HTTPException(status_code=500, detail=str(e))
