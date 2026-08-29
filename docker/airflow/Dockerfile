FROM apache/airflow:3.3.1-python3.12

RUN pip install --no-cache-dir \
      "oracledb==4.0.2" \
      "pydantic==2.13.4" \
      "apache-airflow-providers-standard==1.17.0" \
      "apache-airflow-providers-oracle==4.6.2" \
      "clevercsv==0.8.5" \
      "charset-normalizer==3.5.1" \
      "chardet==7.6.0" \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.3.1/constraints-3.12.txt"

COPY --chown=airflow:0 packages/csv-processor/pyproject.toml packages/csv-processor/pyproject.toml
COPY --chown=airflow:0 packages/csv-processor/src packages/csv-processor/src
RUN pip install --no-cache-dir --no-deps packages/csv-processor/
