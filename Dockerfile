FROM apache/airflow:2.5.0-python3.9

# OS Packages needed by plugins
COPY packages.txt .
USER root
RUN if [[ -s packages.txt ]]; then \
    apt-get update && cat packages.txt | xargs apt-get install -y --no-install-recommends \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*; \
  fi
USER airflow
# Install Airflow using the constraints file
ARG AIRFLOW_VERSION=2.5.0
ARG PYTHON_VERSION=3.9
ARG CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

COPY requirements.txt .
RUN pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
RUN pip install --no-cache-dir -q -r requirements.txt