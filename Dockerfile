FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# System packages needed to compile psycopg, scipy, scikit-learn, xgboost
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential libpq-dev gfortran libopenblas-dev cmake && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE ${PORT:-8501}

CMD ["sh", "-c", "python -m engine.run init || echo 'Schema init skipped (DB not reachable)'; streamlit run streamlit_app/hud.py --server.address=0.0.0.0 --server.port=${PORT:-8501} --server.headless=true"]
