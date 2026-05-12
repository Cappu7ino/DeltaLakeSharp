# -----------------------------------------------------------
# DeltaLakeSharp V2 — Arrow Flight server backed by
# DataFusion (Rust) + delta-rs.  No JVM / Spark required.
# -----------------------------------------------------------
FROM python:3.11-slim

ARG RESOURCE_REAPER_SESSION_ID="00000000-0000-0000-0000-000000000000"
LABEL "org.testcontainers.resource-reaper-session"=$RESOURCE_REAPER_SESSION_ID

ENV FLIGHT_PORT=8815

# ---- Install Python dependencies ----
COPY requirements_v2.txt /tmp/requirements_v2.txt
RUN pip install --no-cache-dir -r /tmp/requirements_v2.txt && \
    rm /tmp/requirements_v2.txt

# ---- Copy application code ----
WORKDIR /opt/delta-table-service
COPY app/ ./app/
COPY v2/run.py .

# ---- Expose Arrow Flight port ----
EXPOSE ${FLIGHT_PORT}

# ---- Entrypoint ----
ENTRYPOINT ["python", "run.py"]
CMD ["--port", "8815", "--host", "0.0.0.0"]
