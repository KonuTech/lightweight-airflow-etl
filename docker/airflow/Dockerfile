FROM apache/airflow:3.3.1-python3.12

# Airflow's constraints-3.3.1 branch is periodically re-pinned upstream (not an
# immutable tag), so its charset-normalizer/chardet pins can drift out from under
# this project's own already-approved exact versions over time (Phase 5 gap found
# during 05-01-PLAN.md execution). clevercsv/charset-normalizer/chardet are
# csv_processor's own detection-library dependencies (packages/csv-processor/
# pyproject.toml, already legitimacy-checkpointed and locked via uv.lock) -- never
# constrained by Airflow's own compatibility matrix, so they're installed in a
# separate, unconstrained pip call to avoid a spurious ResolutionImpossible.
RUN pip install --no-cache-dir \
      "oracledb==4.0.2" \
      "pydantic==2.13.4" \
      "apache-airflow-providers-standard==1.17.0" \
      "apache-airflow-providers-oracle==4.6.2" \
      "faker==40.37.0" \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.3.1/constraints-3.12.txt" \
 && pip install --no-cache-dir \
      "clevercsv==0.8.5" \
      "charset-normalizer==3.5.1" \
      "chardet==7.6.0"

COPY --chown=airflow:0 packages/csv-processor/pyproject.toml packages/csv-processor/pyproject.toml
COPY --chown=airflow:0 packages/csv-processor/src packages/csv-processor/src
RUN pip install --no-cache-dir --no-deps packages/csv-processor/
