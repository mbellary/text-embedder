# text-embedder

The **text-embedder** service is a component of the *Unstruct Modular Data Pipeline* that converts cleaned text into **vector embeddings** and publishes them for **semantic search and retrieval**.

It supports **AWS Bedrock** (e.g., `amazon.titan-embed` / `titan-embedding`), Hugging Face/Sentence-Transformers, and can index vectors into **OpenSearch k-NN**. The service is containerized, integrates with **S3, SQS, DynamoDB**, and exposes **Prometheus** metrics for observability with **Grafana**.

---

## 🧩 Where it fits in the system

| Component | Purpose |
|---|---|
| `file-loader` | Uploads files to S3 and emits SQS jobs |
| `pdf-processor` | Produces raw text + metadata from PDFs |
| `text-extractor` | Cleans/normalizes text and emits extraction events |
| **`text-embedder`** | Generates embeddings from text and indexes to OpenSearch |
| `search` | Serves semantic search using vectors + metadata |
| `infra` (Terraform) | Provisions VPC, ECS, S3, SQS, DynamoDB, OpenSearch, Redis |

**Flow:** `S3 + SQS (processed text)` → **text-embedder** → `OpenSearch (k-NN index)` + optional `S3 (vectors) / DynamoDB (metadata)` → `search`.

---

## 🚀 Features

- Pluggable embedding backends (AWS Bedrock, Sentence-Transformers/TEI)
- Batch & streaming modes (SQS-driven worker)
- OpenSearch k-NN indexing with configurable index mappings
- Idempotent upserts with DynamoDB (optional)
- Prometheus metrics (`/metrics`) + Grafana dashboards
- Local dev with Docker Compose + LocalStack

---

## 📂 Repository Structure

```
text-embedder/
├─ src/text_embedder/
│  ├─ main.py                # Worker entrypoint (SQS poller)
│  ├─ embedder.py            # Model adapters (Bedrock/SBERT/TEI)
│  ├─ indexers/opensearch.py # Vector indexing/upserts and mappings
│  ├─ aws_client.py          # S3/SQS/DynamoDB helpers
│  ├─ metrics.py             # Prometheus metrics
│  └─ __init__.py
├─ Dockerfile.dev
├─ Dockerfile.prod
├─ docker-compose.yml        # app + localstack + prometheus + grafana
├─ prometheus.yml
├─ requirements.txt
├─ pyproject.toml
├─ localstack_data/
├─ grafana_data/
└─ README.md
```

> If file names differ in your repo, adjust the paths above.

---

## ⚙️ Prerequisites

- Python 3.10+
- Docker & Docker Compose
- (Optional) AWS credentials if running against Bedrock (non-local)

---

## 🏁 Quickstart

```bash
git clone https://github.com/mbellary/text-embedder.git
cd text-embedder
docker compose up --build
```

This brings up:
- `text-embedder` worker
- `localstack` (S3, SQS, DynamoDB)
- `prometheus` (:9090) and `grafana` (:3000)

> LocalStack: http://localhost:4566 • Prometheus: http://localhost:9090 • Grafana: http://localhost:3000

---

## 🔧 Configuration

Create a `.env`:

```env
# AWS / LocalStack
AWS_ACCESS_KEY_ID=test
AWS_SECRET_ACCESS_KEY=test
AWS_DEFAULT_REGION=ap-south-1
LOCALSTACK_ENDPOINT=http://localstack:4566

# Queues/Buckets
S3_INPUT_BUCKET=unstruct-extracted-bucket
S3_VECTOR_BUCKET=unstruct-vectors-bucket
SQS_INPUT_QUEUE=unstruct-extraction-events

# OpenSearch (vector index)
OPENSEARCH_ENDPOINT=https://your-opensearch-domain.ap-south-1.es.amazonaws.com
OPENSEARCH_INDEX=unstruct-doc-embeddings
OPENSEARCH_USERNAME=admin            # if basic auth is enabled
OPENSEARCH_PASSWORD=changeme

# Embedding backend
EMBEDDING_BACKEND=bedrock            # bedrock | sbert | tei
EMBEDDING_MODEL=amazon.titan-embedding
EMBEDDING_BATCH_SIZE=16
EMBEDDING_DIM=1024                   # adjust to your model

# Metadata store (optional)
DYNAMODB_TABLE=unstruct-embedding-metadata

# Observability
PROMETHEUS_PORT=9094
LOG_LEVEL=INFO
```

Provision LocalStack resources:

```bash
docker exec -it localstack awslocal s3 mb s3://unstruct-extracted-bucket
docker exec -it localstack awslocal s3 mb s3://unstruct-vectors-bucket
docker exec -it localstack awslocal sqs create-queue --queue-name unstruct-extraction-events
```

---

## 🧠 Example SQS Message

```json
{
  "bucket": "unstruct-extracted-bucket",
  "key": "cleaned/sample_clean.txt",
  "doc_id": "abc123",
  "meta": {"source": "invoice-042.pdf"}
}
```

**text-embedder** will:
1. Download text from S3.
2. Generate an embedding vector.
3. Upsert `{doc_id, vector, metadata}` into OpenSearch index.
4. Optionally upload vector to `s3://unstruct-vectors-bucket/` and write metadata to DynamoDB.
5. Emit Prometheus metrics.

---

## 🧪 Local development (without Docker)

```bash
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
python -m text_embedder
```

---

## 🧰 OpenSearch k-NN Mapping (example)

```json
{
  "settings": {
    "index": {
      "knn": true,
      "knn.algo_param.ef_search": 128
    }
  },
  "mappings": {
    "properties": {
      "doc_id": {"type": "keyword"},
      "text": {"type": "text"},
      "vector": {"type": "knn_vector", "dimension": 1024, "method": {"name": "hnsw", "space_type": "cosinesimil", "engine": "faiss"}},
      "meta": {"type": "object", "enabled": true}
    }
  }
}
```

> Change `dimension` to match your model. Ensure your OpenSearch domain has the **k-NN plugin** enabled.

---

## 📈 Metrics

Exposes Prometheus metrics such as:
- `embedding_jobs_total`
- `embedding_duration_seconds`
- `opensearch_upserts_total`
- `opensearch_failures_total`
- `sqs_messages_consumed_total`

Grafana dashboards are persisted in `grafana_data/`.

---

## 🚀 Deployment (ECS/Fargate)

Deployed by the **infra** Terraform stack:
- ECS Task Definition with task role access to S3, SQS, DynamoDB, Bedrock, OpenSearch
- Service autoscaling by queue depth
- CloudWatch logs
- Prometheus service discovery

CI/CD (GitHub Actions) can build/push images and deploy on merges to `main`.

---

## 🧭 Roadmap

- [ ] Async batching & backpressure control
- [ ] TEI/HF Inference Server integration
- [ ] Rerank step (Titan Rerank) support
- [ ] Dead-letter queue (DLQ) wiring
- [ ] Example Grafana dashboards



---

## 📜 License

Apache License 2.0 — see [`LICENSE`](./LICENSE).

---

## 🧾 Author

**Mohammed Ali** • https://github.com/mbellary

---

> _Part of the **Unstruct Modular Data Pipeline** — a fully containerized, serverless-ready ecosystem for ingestion, processing, and search._
