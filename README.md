# Generic Dataflow Streaming Pipeline

Apache Beam/Dataflow pipeline to ingest files from Google Cloud Storage (GCS) and load them into BigQuery.

## What This Repository Contains

- `load_gcs_to_bq.py`: Main Beam pipeline designed for Dataflow Flex Template runtime parameters (uses `ValueProvider`).
- `load_gcs_to_bq_local.py`: Alternative entrypoint using argparse (useful for local/manual runs).
- `metadata.json`: Flex Template metadata and runtime parameter definitions.
- `schemas/*.json`: Example BigQuery schemas.
- `Dockerfile`: Container image for Dataflow Flex Template.
- `.gitlab-ci.yml`: CI pipeline to upload metadata/schemas, build image, and build Dataflow Flex Template.

## Pipeline Behavior

The pipeline:

1. Matches files from `--input_file` (GCS pattern).
2. Reads each file.
3. Parses supported formats:
   - `csv` / `txt`: first line is treated as header and skipped; columns are mapped using schema field order.
   - `json`: expects a JSON array.
   - `jsonl`: expects one JSON object per line.
4. Appends rows to BigQuery (`WRITE_APPEND`), using `FILE_LOADS`.

For CSV/TXT records, an `insert_date` field is added automatically with current timestamp.

## Prerequisites

- Python 3.9+
- Google Cloud project with:
  - Dataflow API enabled
  - BigQuery API enabled
  - Cloud Storage access
- A service account with permissions for Dataflow, GCS, and BigQuery

## Local Setup

Install dependencies:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pip install "apache-beam[gcp]==2.45.0"
```

## Run Locally (DirectRunner)

```bash
python load_gcs_to_bq_local.py \
  --runner DirectRunner \
  --project processing-452316 \
  --input_file "gs://your-bucket/path/*.csv" \
  --output_table "processing-452316:your_dataset.your_table" \
  --schema "schemas/schema_freework.job_list.json" \
  --separator ","
```

## Run on Dataflow (without Flex Template)

```bash
python load_gcs_to_bq_local.py \
  --runner DataflowRunner \
  --project processing-452316 \
  --service_account_email "your-sa@processing-452316.iam.gserviceaccount.com" \
  --input_file "gs://your-bucket/path/*.csv" \
  --output_table "processing-452316:your_dataset.your_table" \
  --schema "schemas/schema_freework.job_list.json" \
  --separator "," \
  --region europe-west1
```

## Build and Run as Flex Template

Build container image:

```bash
gcloud builds submit --tag gcr.io/processing-452316/dataflow_flex_templates_image .
```

Build Flex Template:

```bash
gcloud dataflow flex-template build gs://dataflow_templ_bucket/metadata.json \
  --image gcr.io/processing-452316/dataflow_flex_templates_image \
  --sdk-language PYTHON \
  --metadata-file metadata.json
```

Run Flex Template job:

```bash
gcloud dataflow flex-template run gcs-to-bq-$(date +%Y%m%d-%H%M%S) \
  --template-file-gcs-location gs://dataflow_templ_bucket/metadata.json \
  --region europe-west1 \
  --parameters input_file="gs://your-bucket/path/*.csv" \
  --parameters output_table="processing-452316:your_dataset.your_table" \
  --parameters schema="schemas/schema_freework.job_list.json" \
  --parameters separator=","
```

## Runtime Parameters

Defined in `metadata.json`:

- `input_file` (required): GCS file pattern (`gs://bucket/path/*.csv`)
- `output_table` (required): BigQuery table (`PROJECT:DATASET.TABLE`)
- `schema` (required): schema JSON path
- `separator` (optional): CSV separator (default `,`)

## Notes and Limitations

- Staging and temp buckets are hardcoded in scripts:
  - `gs://dataflow_bucket_processing-452316/staging`
  - `gs://dataflow_bucket_processing-452316/temp`
- Region is hardcoded to `europe-west1` in both Python entrypoints.
- CSV parsing is split-based and does not handle quoted separators robustly.
- `requirements.txt` does not include Apache Beam; install it separately (or add it to requirements).

## CI/CD

GitLab CI (`.gitlab-ci.yml`) currently defines three stages:

1. Upload `metadata.json` and `schemas/` to GCS
2. Build and push container image with Cloud Build
3. Build Dataflow Flex Template

Ensure `GCP_SERVICE_KEY` is configured in CI variables (base64-encoded service account key).
