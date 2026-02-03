## ------------------------------- Builder Stage ------------------------------ ##
FROM python:3.13-bookworm AS builder

# Instalar dependencias para build
RUN apt-get update && apt-get install --no-install-recommends -y \
    build-essential \
    curl && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Instalar uv
ADD https://astral.sh/uv/install.sh /install.sh
RUN chmod -R 655 /install.sh && /install.sh && rm /install.sh
ENV PATH="/root/.local/bin:${PATH}"

WORKDIR /app

# Copiar archivos de dependencia
COPY pyproject.toml uv.lock ./

# Sincronizar dependencias para build
RUN uv sync --frozen --no-dev --no-install-project

## ------------------------------- Production Stage ------------------------------ ##
FROM python:3.13-slim-bookworm AS production

# Instalar dependencias para producción
RUN apt-get update && apt-get install --no-install-recommends -y \
    curl \
    procps && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Instalar uv en producción para poder instalar el proyecto
ADD https://astral.sh/uv/install.sh /install.sh
RUN chmod -R 655 /install.sh && /install.sh && rm /install.sh
ENV PATH="/root/.local/bin:${PATH}"

# Se crea usuario para producción
RUN useradd --create-home --shell /bin/bash appuser

WORKDIR /app

# Se copia entorno virtual desde builder
COPY --from=builder /app/.venv .venv

# Se copia código de aplicación
COPY --chown=appuser:appuser ./src ./src
COPY --chown=appuser:appuser ./pyproject.toml ./

# Se instala el proyecto
RUN uv pip install --no-deps -e . --python .venv/bin/python

USER appuser

# Variables de entorno
ENV PATH="/app/.venv/bin:$PATH"
ENV PYTHONPATH="/app/src:${PYTHONPATH}"
ENV PYTHONUNBUFFERED=1

# Comando de salud
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import sys; sys.exit(0)"
