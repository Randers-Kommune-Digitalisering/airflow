FROM apache/airflow:2.11.0

USER root

RUN apt-get update && apt-get install -y \
    wget \
    ca-certificates \
    gnupg \
    curl \
    libicu-dev \
 && rm -rf /var/lib/apt/lists/*

# COPY cert/CA-Den Danske Stat OCES rod-CA.cer /usr/local/share/ca-certificates/den_danske_stat.crt
COPY Certificates/den_danske_stat_rod_ca.cer /Certificates/den_danske_stat.crt
COPY Certificates/ADG_PROD_Adgangsstyring_2.cer /Certificates/ADG_PROD_Adgangsstyring_2.cer
COPY Certificates/YDI_PROD_Ydelsesindeks_2.cer /Certificates/YDI_PROD_Ydelsesindeks_2.cer
COPY Certificates/client.p12 /Certificates/client.p12

# Installér i trust store
RUN update-ca-certificates

# .NET runtime
RUN curl -sSL https://dot.net/v1/dotnet-install.sh -o dotnet-install.sh \
    && chmod +x dotnet-install.sh \
    && ./dotnet-install.sh --channel 9.0 --runtime dotnet --install-dir /usr/share/dotnet \
    && rm dotnet-install.sh

ENV DOTNET_ROOT=/usr/share/dotnet
ENV PATH="$PATH:/usr/share/dotnet"
ENV DOTNET_SYSTEM_GLOBALIZATION_INVARIANT=false

USER airflow

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt