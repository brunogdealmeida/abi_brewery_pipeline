FROM quay.io/astronomer/astro-runtime:9.2.0

USER root

# Instala pacotes do sistema (incluindo Java)
RUN apt-get update && \
    apt-get install -y openjdk-11-jdk libgeos-dev && \
    apt-get clean


# Instala dependências Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

USER astro
