# The lab's backend: Sparrow's, with this lab's schema and pipelines installed.
FROM ghcr.io/earthcubegeochron/sparrow/backend

COPY pyproject.toml /lab/
COPY laserchron /lab/laserchron
RUN pip install --no-cache-dir /lab
