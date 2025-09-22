import json

from text_embedder.aws_clients import get_aboto3_client
from text_embedder.config import BEDROCK_MODEL_ID


async def invoke_bedrock_embedding(text: str):
    """
    Calls Bedrock to get embeddings for provided text.
    Returns list[float]
    """
    # Convert to the invocation shape your model expects.
    # Many embedding models accept: {"input": "<text>"} or a JSON wrapper.
    client = await get_aboto3_client("bedrock-runtime")

    try:
        model_id = BEDROCK_MODEL_ID
        # Prepare input; model-specific. This is generic JSON body.
        payload = {"input": text}
        resp = await client.invoke_model(
            modelId=model_id,
            contentType="application/json",
            accept="application/json",
            body=json.dumps(payload).encode("utf-8"),
        )
        # resp is a streaming/binary body. Read and parse
        body_bytes = await resp['body'].read()
        data = json.loads(body_bytes.decode("utf-8"))
        # Assume model returns {"embeddings": [ ... ]} or {"embedding":[...]}
        if "embedding" in data:
            return data["embedding"]
        if "embeddings" in data:
            return data["embeddings"]
        # If model returns text, attempt to parse numeric list
        if isinstance(data, dict):
            # try common keys
            for key in data:
                if isinstance(data[key], list):
                    return data[key]
        raise RuntimeError(f"Unexpected bedrock response: {data}")
    finally:
        await client.__aexit__(None, None, None)