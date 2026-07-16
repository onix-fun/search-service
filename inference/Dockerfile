FROM python:3.12-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 HF_HOME=/models

# Install PyTorch from the CPU index first.  Leaving the transitive
# `torch>=1.11` requirement unresolved makes pip select the newest Linux
# wheel, which also pulls the CUDA toolkit into this CPU-only service.
RUN pip install --no-cache-dir \
      --index-url https://download.pytorch.org/whl/cpu \
      torch==2.7.1+cpu && \
    pip install --no-cache-dir \
      fastapi==0.116.1 \
      uvicorn==0.35.0 \
      sentence-transformers==5.0.0
WORKDIR /app
COPY server.py /app/server.py
EXPOSE 8080
CMD ["uvicorn", "server:app", "--host", "0.0.0.0", "--port", "8080"]
