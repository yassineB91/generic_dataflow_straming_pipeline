FROM apache/beam_python3.9_sdk:2.45.0

WORKDIR /pipeline

# Copy the pipeline code
COPY load_gcs_to_bq.py .
COPY metadata.json .
COPY requirements.txt .
COPY schemas/schema_*.json ./schemas/
RUN pip install -r requirements.txt

# entrypoint
ENTRYPOINT ["python", "/pipeline/load_gcs_to_bq.py"]