import asyncio
from text_embedder.opensearch_client import purge_all_documents
from text_embedder.logger import get_logger

logger = get_logger("text_embedder.purge")

async def main():
    try:
        result = await purge_all_documents()
        logger.info("Purge successful: %s", result)
    except Exception as e:
        logger.exception("Purge failed: %s", e)

def run():
    asyncio.run(main())

if __name__ == "__main__":
    run()
