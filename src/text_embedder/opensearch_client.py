import aiohttp
import asyncio
import json
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.credentials import Credentials, ReadOnlyCredentials
from botocore.session import Session as BotoSession
from text_embedder.config import AWS_REGION, OPENSEARCH_HOST, OPENSEARCH_INDEX, BEDROCK_EMBEDDING_OUTPUT_DIM
from text_embedder.logger import get_logger
import boto3

logger = get_logger("text_embedder.opensearch")

def _sign_request(method: str, url: str, body: bytes = b"", service="es", region=None):
    """
    Create headers with AWS SigV4 signature for raw HTTP request to OpenSearch.
    Returns dict(headers).
    """
    if OPENSEARCH_HOST.startswith("http://localhost"):
        return {}

    region = AWS_REGION
    # Get credentials synchronously from boto3
    boto_session = boto3.session.Session()
    creds = boto_session.get_credentials()
    frozen = creds.get_frozen_credentials()
    #aws_credentials = Credentials(creds.access_key, creds.secret_key, creds.token)
    request = AWSRequest(method=method, url=url, data=body)
    SigV4Auth(frozen, service, region).add_auth(request)
    return dict(request.headers.items())

async def create_index():
    """
    Create a vector-aware index for embeddings.
    """
    url = f"{OPENSEARCH_HOST}/{OPENSEARCH_INDEX}"

    body = {
        "settings": {"index": {"knn": True}},
        "mappings": {
            "properties": {
                "file_key": {"type": "keyword"},
                "page_num": {"type": "integer"},
                "text": {"type": "text"},
                "metadata": {"type": "object"},
                "embedding": {"type": "knn_vector", "dimension": BEDROCK_EMBEDDING_OUTPUT_DIM},
            }
        },
    }
    body_bytes = json.dumps(body).encode("utf-8")
    headers = _sign_request("PUT", url, body_bytes, service="es")
    headers["Content-Type"] = "application/json"

    async with aiohttp.ClientSession() as session:
        async with session.put(url, data=body_bytes, headers=headers) as resp:
            text = await resp.text()
            if resp.status not in (200, 201):
                logger.error("Failed to create index: %s", text)
                raise RuntimeError(f"Index creation failed: {resp.status} {text}")
            logger.info("Index created: %s", text)
            return json.loads(text)


async def index_document(doc_id: str, document: dict):
    url = f"{OPENSEARCH_HOST}/{OPENSEARCH_INDEX}/_doc/{doc_id}"
    body = json.dumps(document).encode("utf-8")
    headers = _sign_request("PUT", url, body, service="es")
    async with aiohttp.ClientSession() as session:
        async with session.put(url, data=body, headers={**headers, "Content-Type":"application/json"}) as resp:
            text = await resp.text()
            if resp.status not in (200,201):
                logger.error("Index failed %s %s", resp.status, text)
                raise RuntimeError(f"Index failed: {resp.status} {text}")
            return json.loads(text)

async def search(query: dict, size: int = 10):
    url = f"{OPENSEARCH_HOST}/{OPENSEARCH_INDEX}/_search"
    body = json.dumps(query).encode("utf-8")
    headers = _sign_request("POST", url, body, service="es")
    async with aiohttp.ClientSession() as session:
        async with session.post(url, data=body, headers={**headers, "Content-Type":"application/json"}) as resp:
            text = await resp.text()
            if resp.status != 200:
                logger.error("Search failed %s %s", resp.status, text)
                raise RuntimeError(f"Search failed: {resp.status} {text}")
            return json.loads(text)

async def vector_search(vector: list[float], k: int = 5):
    """
    Run a k-NN vector similarity search in OpenSearch.
    """
    url = f"{OPENSEARCH_HOST}/{OPENSEARCH_INDEX}/_search"
    body = {
        "size": k,
        "query": {
            "knn": {
                "embedding": {
                    "vector": vector,
                    "k": k
                }
            }
        }
    }

    body_bytes = json.dumps(body).encode("utf-8")
    headers = _sign_request("POST", url, body_bytes, service="es")

    async with aiohttp.ClientSession() as session:
        async with session.post(
            url, data=body_bytes, headers={**headers, "Content-Type": "application/json"}
        ) as resp:
            text = await resp.text()
            if resp.status != 200:
                logger.error("Vector search failed %s %s", resp.status, text)
                raise RuntimeError(f"Vector search failed: {resp.status} {text}")
            return json.loads(text)


async def delete_document(doc_id: str):
    url = f"{OPENSEARCH_HOST}/{OPENSEARCH_INDEX}/_doc/{doc_id}"
    headers = _sign_request("DELETE", url, b"", service="es")
    async with aiohttp.ClientSession() as session:
        async with session.delete(url, headers=headers) as resp:
            text = await resp.text()
            if resp.status not in (200, 404):
                raise RuntimeError(f"Delete failed: {resp.status} {text}")
            return json.loads(text)

async def index_exists() -> bool:
    """
    Check if the OpenSearch index already exists.
    """
    url = f"{OPENSEARCH_HOST}/{OPENSEARCH_INDEX}"
    headers = _sign_request("HEAD", url, service="es")

    logger.info("Checking if the index exists..")
    async with aiohttp.ClientSession() as session:
        async with session.head(url, headers=headers) as resp:
            if resp.status == 200:
                return True
            elif resp.status == 404:
                return False
            else:
                text = await resp.text()
                logger.error("Index existence check failed: %s %s", resp.status, text)
                raise RuntimeError(f"Index existence check failed: {resp.status} {text}")


async def ensure_index(dimension: int):
    """
    Ensure the index exists with the correct vector dimension.
    Will only create if missing.
    """
    exists = await index_exists()
    if exists:
        logger.info("Index '%s' already exists", OPENSEARCH_INDEX)
        return

    logger.info("Creating index '%s' with dimension %s", OPENSEARCH_INDEX, dimension)
    return await create_index()

async def purge_all_documents():
    """
    Delete all documents from the OpenSearch index without dropping the mapping.
    """
    url = f"{OPENSEARCH_HOST}/{OPENSEARCH_INDEX}/_delete_by_query"
    body = {"query": {"match_all": {}}}
    body_bytes = json.dumps(body).encode("utf-8")

    headers = _sign_request("POST", url, body_bytes, service="es")
    headers["Content-Type"] = "application/json"

    async with aiohttp.ClientSession() as session:
        async with session.post(url, data=body_bytes, headers=headers) as resp:
            text = await resp.text()
            if resp.status != 200:
                raise RuntimeError(f"Purge failed: {resp.status} {text}")
            return json.loads(text)
