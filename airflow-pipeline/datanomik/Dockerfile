FROM apache/airflow:3.3.1-python3.12 AS runtime
COPY --chown=airflow:root requirements.txt /tmp/datanomik-requirements.txt
RUN pip install --no-cache-dir -r /tmp/datanomik-requirements.txt \
    --constraint /home/airflow/constraints.txt \
    && pip check

FROM runtime AS test
COPY --chown=airflow:root requirements-test.txt /tmp/requirements-test.txt
COPY --chown=airflow:root requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements-test.txt \
    --constraint /home/airflow/constraints.txt \
    && pip check
