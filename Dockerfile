FROM apache/airflow:2.11.2

USER root

RUN apt-get update && apt-get install -y \
    ca-certificates \
    curl \
    gnupg \
    && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /etc/apt/keyrings \
 && curl -fsSL https://packages.microsoft.com/keys/microsoft.asc \
    | gpg --dearmor -o /etc/apt/keyrings/microsoft.gpg

RUN echo "deb [arch=amd64 signed-by=/etc/apt/keyrings/microsoft.gpg] \
    https://packages.microsoft.com/debian/11/prod bullseye main" \
    > /etc/apt/sources.list.d/microsoft-prod.list

RUN apt-get update \
 && apt-get install -y dotnet-runtime-9.0 \
 && rm -rf /var/lib/apt/lists/*

RUN dotnet --list-runtimes

COPY ["dags/certs/CA-Den Danske Stat OCES rod-CA.cer", "/usr/local/share/ca-certificates/CA-Den-Danske-Stat-OCES-rod-CA.crt"]
COPY ["dags/certs/CA-Den Danske Stat OCES udstedende-CA 1.cer", "/usr/local/share/ca-certificates/CA-Den-Danske-Stat-OCES-udstedende-CA-1.crt"]
RUN update-ca-certificates

USER airflow

ENV CLIENT_CERT=
ENV CVR_NUMBER="29189668"
ENV CERT_BASE_PATH=/opt/airflow/dags/repo/dags/certs

COPY requirements.txt /requirements.txt

RUN PYTHON_VERSION=$(python -c "import sys; print(str(sys.version_info.major)+'.'+str(sys.version_info.minor))") && CONSTRAINTS_FILE="/home/airflow/.local/share/airflow/constraints-${PYTHON_VERSION}.txt" && AIRFLOW_VERSION=$(python -c "import airflow; print(airflow.__version__)") && ( [ -f "${CONSTRAINTS_FILE}" ] && cat "${CONSTRAINTS_FILE}" > /tmp/custom_constraints.txt || curl -fsSL "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt" -o /tmp/custom_constraints.txt ) && while read -r line; do [[ "$line" =~ ^[[:space:]]*# ]] && continue; [[ -z "$line" ]] && continue; pkg=$(echo "$line" | sed -E 's/(==|>=|<=|<|>).*//' | xargs); sed -i -E "/^${pkg}==/d" /tmp/custom_constraints.txt; done < /requirements.txt

RUN pip install --no-cache-dir -r /requirements.txt --constraint /tmp/custom_constraints.txt

USER root
RUN PYTHON_VERSION=$(python -c "import sys; print(str(sys.version_info.major)+'.'+str(sys.version_info.minor))") && PYTHONPATH="/home/airflow/.local/lib/python${PYTHON_VERSION}/site-packages" /home/airflow/.local/bin/playwright install-deps chromium
USER airflow
RUN python -m playwright install chromium
