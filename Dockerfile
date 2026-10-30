FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY store.py decide.py a2a_app.py main.py ./
ENV DB_PATH=/tmp/a2a.db
# ONE worker: in-flight de-duplication and the cancel/result race rely on a single process.
CMD ["sh", "-c", "uvicorn main:app --host 0.0.0.0 --port ${PORT:-8000} --workers 1 --proxy-headers --forwarded-allow-ips='*'"]
