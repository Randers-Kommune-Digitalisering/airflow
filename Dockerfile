FROM apache/airflow:2.11.0

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

RUN python -c ' \
import re; \
reqs = [re.split(r"==|>=|<=|<|>", line)[0].strip() for line in open("/requirements.txt") if line.strip() and not line.startswith("#")]; \
lines = open("/constraints-3.12.txt").readlines(); \
filtered = [l for l in lines if not any(re.search(r"\b" + re.escape(r) + r"\b", l, re.IGNORECASE) for r in reqs)]; \
open("/tmp/custom_constraints.txt", "w").writelines(filtered); \
'

RUN pip install --no-cache-dir -r /requirements.txt --constraint /tmp/custom_constraints.txt
