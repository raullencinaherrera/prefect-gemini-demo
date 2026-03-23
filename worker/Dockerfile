# worker/Dockerfile
FROM prefecthq/prefect:3-latest

# (Opcional pero recomendable) Asegura certificados del sistema
USER root
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
 && update-ca-certificates \
 && rm -rf /var/lib/apt/lists/*

# Copia primero los requirements y LUEGO instala
COPY requirements.txt /tmp/requirements.txt
RUN pip install --trusted-host pypi.org --trusted-host files.pythonhosted.org --no-cache-dir -r /tmp/requirements.txt

WORKDIR /opt/prefect
