# Single image for both roles; the compose service decides which command runs.
FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# Copy requirements first so the dependency layer is cached across code edits.
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app

# Do not run as root.
RUN useradd --create-home --uid 10001 appuser
USER appuser

EXPOSE 8000 9100

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
