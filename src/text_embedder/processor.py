import asyncio
import json
import uuid
from text_embedder.utils import fetch_s3_jsonl, change_dynamodb_status
from text_embedder.embedder import invoke_bedrock_embedding, invoke_embedding_model
from text_embedder.opensearch_client import index_document, create_index, ensure_index
from text_embedder.logger import get_logger
from text_embedder.config import CONCURRENCY
from prometheus_client import Counter, Histogram

logger = get_logger("text_embedder.processor")

EMBED_SUCCESS = Counter("embed_success_count", "Number of pages embedded successfully")
EMBED_FAILURE = Counter("embed_failure_count", "Number of pages failed embedding")
INDEX_LATENCY = Histogram("index_latency_seconds", "Time taken to index document")

_index_created = False  # module-level flag


async def process_file(s3_key: str):
    """
    1. mark processing in dynamo
    2. fetch pages
    3. embed pages in batches
    4. ensure index exists (with embedding dim)
    5. index each page (document contains vector + metadata)
    6. update dynamodb to done or failed
    """
    logger.info("Start processing %s", s3_key)
    await change_dynamodb_status(s3_key, "processing")
    try:
        pages = await fetch_s3_jsonl(s3_key)
        # batch pages into batches of batch_size for embeding if desired.
        tasks = []
        sem = asyncio.Semaphore(CONCURRENCY)
        async def embed_and_index(page):
            #print(f"DEBUG pages: {page}")
            page = json.loads(page)
            async with sem:
                logger.info(f"starting text embedding.")
                try:
                    text = page.get("text", "")
                    vector = await invoke_embedding_model(text)

                    # build document
                    doc_id = page.get("id") or str(uuid.uuid4())
                    document = {
                        "token_count": page.get("token_count"),
                        "text": text,
                        "metadata": page.get("metadata", {}),
                        "embedding": vector
                    }
                    with INDEX_LATENCY.time():
                        await index_document(doc_id, document)
                    EMBED_SUCCESS.inc()
                except Exception as v:
                    EMBED_FAILURE.inc()
                    logger.exception("Failed embed/index page %s: %s", page.get("page_num"), v)
                    raise

        # Mistral OCR JSON data
        for p in pages:
            tasks.append(asyncio.create_task(embed_and_index(p['chunks'])))

        # await all tasks; if any fail, propagate
        results = await asyncio.gather(*tasks, return_exceptions=True)
        # Check exceptions
        errors = [r for r in results if isinstance(r, Exception)]
        if errors:
            raise RuntimeError(f"{len(errors)} pages failed to embed/index")
        # success
        await change_dynamodb_status(s3_key, "done")
        logger.info("Processing completed for %s", s3_key)
    except Exception as e:
        logger.exception("Processing failed for %s", s3_key)
        await change_dynamodb_status(s3_key, "failed", extra={"error": str(e)})
        raise
